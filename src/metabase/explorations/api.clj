(ns metabase.explorations.api
  "`/api/exploration` routes."
  (:require
   [java-time.api :as t]
   [metabase.api.common :as api]
   [metabase.api.macros :as api.macros]
   [metabase.api.routes.common :refer [+auth]]
   [metabase.collections.models.collection :as collection]
   [metabase.events.core :as events]
   [metabase.explorations.core :as explorations]
   [metabase.explorations.models.exploration :as expl.model]
   [metabase.explorations.queues :as explorations.queues]
   [metabase.request.core :as request]
   [metabase.util.malli.registry :as mr]
   [metabase.util.malli.schema :as ms]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;;; ----------------------------------------- helpers -----------------------------------------

(defn- get-exploration-or-404 [id]
  (api/check-404 (t2/select-one :model/Exploration :id id)))

(defn- check-destination-collection-perms!
  "When `updates` moves the exploration to a different `collection_id`, verify the current
  user has write perms on the destination (collection or root). Source-side perms are already
  enforced by the parent `api/write-check` against the exploration itself, which via
  `:perms/use-parent-collection-perms` requires write on the source collection."
  [{old-coll :collection_id} updates]
  (when (and (contains? updates :collection_id)
             (not= old-coll (:collection_id updates)))
    (let [new-coll (:collection_id updates)]
      (if new-coll
        (api/write-check :model/Collection new-coll)
        (api/write-check collection/root-collection)))))

(defn- hydrate-exploration [exploration]
  (t2/hydrate exploration :creator :can_write :collection :threads))

(defn- positional-rows
  "Stamp `:exploration_thread_id` and a 0-based `:position` onto each row in `rows`."
  [thread-id rows]
  (map-indexed (fn [i row]
                 (assoc row :exploration_thread_id thread-id :position i))
               rows))

(defn- insert-blocks!
  "Persist the FE's Research-plan blocks verbatim — one `ExplorationBlock` row per
   block, in payload order. Each block keeps its own `:metrics`/`:dimensions` selection;
   the planners cross metrics with dimensions only within a block. No dedup across blocks:
   a metric or dimension appearing in two blocks is stored on both."
  [thread-id blocks]
  (when (seq blocks)
    (t2/insert! :model/ExplorationBlock
                (positional-rows thread-id
                                 (map #(select-keys % [:type :metrics :dimensions]) blocks)))))

;;; ----------------------------------------- schemas -----------------------------------------

(mr/def ::HydratedThread
  "Schema for an Exploration thread."
  [:map
   [:id             ms/PositiveInt]
   [:exploration_id ms/PositiveInt]
   [:prompt         {:optional true} [:maybe :string]]
   [:position       ms/IntGreaterThanOrEqualToZero]
   [:started_at     {:optional true} [:maybe :any]]])

(mr/def ::HydratedExploration
  "Schema for an Exploration with hydrated creator and threads."
  [:map
   [:id            ms/PositiveInt]
   [:name          :string]
   [:description   {:optional true} [:maybe :string]]
   [:creator_id    ms/PositiveInt]
   [:creator       {:optional true} [:maybe :map]]
   [:collection_id {:optional true} [:maybe ms/PositiveInt]]
   [:archived      {:optional true} :boolean]
   [:threads       {:optional true} [:maybe [:sequential ::HydratedThread]]]
   [:created_at    {:optional true} [:maybe :any]]
   [:updated_at    {:optional true} [:maybe :any]]])

(mr/def ::ExplorationSummary
  "Lightweight row for the `GET /mine` list. No threads/queries — just the metadata
  needed to render a list entry, plus `current_user_last_touched_at`, the timestamp the list is
  sorted by (the caller's own most-recent touch of this exploration, composed across the
  exploration's revisions and its creation)."
  [:map
   [:id                           ms/PositiveInt]
   [:name                         ms/NonBlankString]
   [:description                  {:optional true} [:maybe :string]]
   [:creator_id                   ms/PositiveInt]
   ;; The `:creator` batched-hydrate selects a User subset, or `{}` when the creator can't be
   ;; resolved — hence every key is optional.
   [:creator                      {:optional true}
    [:maybe [:map
             [:id         {:optional true} ms/PositiveInt]
             [:email      {:optional true} ms/NonBlankString]
             [:first_name {:optional true} [:maybe :string]]
             [:last_name  {:optional true} [:maybe :string]]]]]
   [:collection_id                {:optional true} [:maybe ms/PositiveInt]]
   ;; `nil` for root-collection explorations; otherwise the hydrated collection (open map — only
   ;; the fields the FE needs are asserted).
   [:collection                   {:optional true}
    [:maybe [:map
             [:id   ms/PositiveInt]
             [:name ms/NonBlankString]]]]
   [:archived                     {:optional true} :boolean]
   [:created_at                   ms/TemporalInstant]
   [:updated_at                   ms/TemporalInstant]
   [:current_user_last_touched_at ms/TemporalInstant]])

(mr/def ::MineResponse
  "Paginated envelope for `GET /mine`, mirroring the collection-items index shape. `:total` is the
  count after the membership, permission, and archived filters; `:limit`/`:offset` echo the
  `offset-paging` middleware (both nil when the request is unpaged)."
  [:map
   [:total  ms/IntGreaterThanOrEqualToZero]
   [:limit  [:maybe ms/IntGreaterThanOrEqualToZero]]
   [:offset [:maybe ms/IntGreaterThanOrEqualToZero]]
   [:data   [:sequential ::ExplorationSummary]]])

(def ^:private MetricSelection
  [:map
   [:card_id ms/PositiveInt]
   [:dimension_mappings {:optional true} [:maybe [:sequential :map]]]])

(def ^:private DimensionSelection
  [:map
   [:dimension_id   ms/NonBlankString]
   [:display_name   {:optional true} [:maybe :string]]
   [:effective_type {:optional true} [:maybe :string]]
   [:semantic_type  {:optional true} [:maybe :string]]])

(def ^:private BlockSelection
  "One Research-plan area on the FE — either a metric area (one primary metric + chosen dimensions)
   or a dimension area (the dimension's group + referencing metrics). Persisted verbatim as one
   `ExplorationBlock` row; the planners cross this block's metrics with this block's
   dimensions only. The sidebar heading is computed read-side (the `:name` of an
   `ExplorationBlockNode`), not supplied here."
  [:map
   ;; Whether the block is anchored on its metric or its dimension. The read side
   ;; uses this to build the sidebar heading + sub-item names.
   [:type       {:optional true} [:maybe [:enum "metric" "dimension"]]]
   [:metrics    {:optional true} [:maybe [:sequential MetricSelection]]]
   [:dimensions {:optional true} [:maybe [:sequential DimensionSelection]]]])

(def ^:private CreateExploration
  "Body schema for `POST /api/exploration`.

   The FE sends one entry per Research-plan block (`:blocks` — each a metric/dimension
   area), each persisted verbatim."
  [:map
   [:name          expl.model/ExplorationName]
   [:description   {:optional true} [:maybe :string]]
   [:prompt        {:optional true} [:maybe :string]]
   [:collection_id {:optional true} [:maybe ms/PositiveInt]]
   [:blocks        {:optional true} [:maybe [:sequential BlockSelection]]]])

(def ^:private UpdateExploration
  "Body schema for `PUT /api/exploration/:id`. All fields are optional; only the keys the client
  actually includes are forwarded to the underlying `t2/update!`. `collection_id` may be `nil`
  to move the exploration to the root collection (\"Our Analytics\"). `collection_position` may
  be `nil` to unpin the exploration."
  [:map
   [:name                {:optional true} expl.model/ExplorationName]
   [:description         {:optional true} [:maybe :string]]
   [:archived            {:optional true} :boolean]
   [:collection_id       {:optional true} [:maybe ms/PositiveInt]]
   [:collection_position {:optional true} [:maybe ms/PositiveInt]]])

;;; ----------------------------------------- /dimensions schemas + helpers -----------------------------------------

(mr/def ::ExplorationMetric
  "Schema for a metric in the /dimensions response: dimensions referenced by id only."
  [:map
   [:id            ms/PositiveInt]
   [:name          :string]
   [:description   [:maybe :string]]
   [:collection_id [:maybe ms/PositiveInt]]
   [:collection    {:optional true} [:maybe [:map
                                             [:id [:maybe ms/PositiveInt]]
                                             [:name :string]]]]
   [:dimension_ids        [:sequential :any]]
   [:dimension_mappings   {:optional true} [:maybe [:sequential :map]]]
   [:database_id          {:optional true} [:maybe ms/PositiveInt]]
   [:result_column_name   {:optional true} [:maybe :string]]
   [:in_library           {:optional true} :boolean]])

(mr/def ::ExplorationDimensionGroup
  "Schema for a dimension group in the /dimensions response. A group bundles together dimensions that
   refer to the same underlying source (same field/binning) so the FE can show a single user-facing
   entry while still tracking the actual per-metric dimensions needed by `start exploration`."
  [:map
   [:name                       :string]
   [:dimension_interestingness  [:maybe number?]]
   [:dimensions                 [:sequential :map]]])

(mr/def ::DimensionsResponse
  "Schema for GET /dimensions: metrics referencing dimensions by id, plus the grouped dimension list."
  [:map
   [:metrics          [:sequential ::ExplorationMetric]]
   [:dimension_groups [:sequential ::ExplorationDimensionGroup]]])

;;; ----------------------------------------- endpoints -----------------------------------------

(api.macros/defendpoint :post "/" :- ::HydratedExploration
  "Create a new exploration with a single thread, persist the user's selected metrics and
  dimensions, and stamp the thread as started. Actual planning is async; this endpoint returns
  immediately with an empty queries list. Clients should poll `GET /:id/queries` until rows appear.

  Accepts the per-area `:blocks` payload (one entry per Research-plan block), persisted verbatim."
  [_route-params
   _query-params
   {:keys [name description prompt collection_id blocks]} :- CreateExploration]
  (api/create-check :model/Exploration {:collection_id collection_id})
  ;; Block metric-card references are persisted verbatim and read back unfiltered
  ;; (planning context, thread hydration), so attach time is the
  ;; permission boundary: every referenced id must exist (404) and be readable (403) by the creator.
  (doseq [card-id (distinct (mapcat #(map :card_id (:metrics %)) blocks))]
    (api/read-check :model/Card card-id))
  (let [persisted
        (t2/with-transaction [_]
          (let [exploration (first (t2/insert-returning-instances! :model/Exploration
                                                                   {:name          name
                                                                    :description   description
                                                                    :collection_id collection_id
                                                                    :creator_id    api/*current-user-id*}))
                ;; `started_at` marks the thread as started (past the draft phase). Planning itself
                ;; is kicked off by the `start-thread!` enqueue below — its plan message rides a
                ;; `:transactional :require` queue, so it publishes only once this whole transaction,
                ;; including the dependent block rows below, commits atomically.
                ;; The plan listener therefore can never observe (or plan) a half-built thread.
                thread      (first (t2/insert-returning-instances! :model/ExplorationThread
                                                                   {:exploration_id (:id exploration)
                                                                    :prompt         prompt
                                                                    :position       0
                                                                    :started_at     (t/offset-date-time)}))
                tid         (:id thread)]
            (insert-blocks! tid blocks)
            (explorations.queues/start-thread! tid)
            (t2/select-one :model/Exploration :id (:id exploration))))]
    ;; Published after the transaction commits (matching PUT) so listeners can never observe an
    ;; exploration that isn't visible to other connections yet.
    (events/publish-event! :event/exploration-create
                           {:object persisted :user-id api/*current-user-id*})
    (hydrate-exploration persisted)))

(api.macros/defendpoint :get "/dimensions" :- ::DimensionsResponse
  "Hydrated metrics plus a deduplicated dimension list, for the Exploration data modal.

  Optional `q` filters case-insensitively across metric name and dimension display-name."
  [_route-params
   {:keys [q]} :- [:maybe [:map [:q {:optional true} [:maybe ms/NonBlankString]]]]]
  (explorations/exploration-data {:q q}))

(defn- my-explorations-honeysql
  "HoneySQL for the explorations `user-id` created or edited, ordered by that user's most-recent
  touch (descending). \"Touch\" is the union of two streams, all attributed to the user:

    1. the user's `Exploration` revisions (metadata / structure edits),
    2. `exploration.created_at` for explorations the user created — creation is a touch, and
       `created_at` stays reliable even after the creation revision ages out of the
       `revision/max-revisions` cap.

  Membership is the inner join to the per-exploration MAX-timestamp aggregate (`agg`), so an
  exploration appears iff the user produced at least one touch. The MAX is the sort key (no
  `GREATEST`, whose NULL semantics differ across app DBs). `current_user_last_touched_at` is
  therefore non-null for every row. `archived = false` and the read-permission visibility filter
  on `collection_id` (which drops explorations moved into collections the user can no longer see)
  keep the `COUNT(*) OVER ()` total honest. `limit`/`offset` are appended only when paged.

  `my-touches` is embedded as a derived table inside `agg` rather than as a sibling CTE: a second
  `:with` binding that selects from the first (`agg` reading `my_touches`) silently returns no
  rows under our HoneySQL/H2 stack."
  [user-id limit offset]
  (let [my-touches {:union-all
                    [{:select [[:model_id :eid] [:timestamp :ts]]
                      :from   [:revision]
                      :where  [:and [:= :model "Exploration"] [:= :user_id user-id]]}
                     {:select [[:id :eid] [:created_at :ts]]
                      :from   [:exploration]
                      :where  [:= :creator_id user-id]}]}
        agg        {:select   [:eid [[:max :ts] :max_ts]]
                    :from     [[my-touches :my_touches]]
                    :group-by [:eid]}]
    (cond-> {:select   [:exploration.*
                        [:agg.max_ts :current_user_last_touched_at]
                        [[:over [[:count :*] {} :total_count]]]]
             :from     [:exploration]
             :join     [[agg :agg] [:= :agg.eid :exploration.id]]
             :where    [:and
                        [:= :exploration.archived false]
                        (collection/visible-collection-filter-clause :exploration.collection_id)]
             :order-by [[:current_user_last_touched_at :desc] [:exploration.id :desc]]}
      limit  (assoc :limit limit)
      offset (assoc :offset offset))))

;; Declared before `/:id` so the literal route wins — `"mine"` would otherwise fail the
;; `:id` PositiveInt coercion rather than fall through here.
(api.macros/defendpoint :get "/mine" :- ::MineResponse
  "Explorations the current user created or edited, most-recently-touched first, paginated.

  \"Touched\" composes the user's own edits to the exploration and its creation — see
  [[my-explorations-honeysql]]. Explorations that were moved into a collection the user can no
  longer read are excluded, as are archived ones. Returns the collection-items envelope:
  `{:total :limit :offset :data}`."
  []
  (let [limit  (request/limit)
        offset (request/offset)
        rows   (-> (t2/select :model/Exploration (my-explorations-honeysql api/*current-user-id* limit offset))
                   (t2/hydrate :creator :collection))]
    {:total  (or (-> rows first :total_count) 0)
     :limit  limit
     :offset offset
     :data   (mapv #(dissoc % :total_count) rows)}))

(api.macros/defendpoint :get "/:id" :- ::HydratedExploration
  "Fetch an exploration with its thread."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]]
  (let [expl (api/read-check (get-exploration-or-404 id))]
    (hydrate-exploration expl)))

(api.macros/defendpoint :put "/:id" :- ::HydratedExploration
  "Update an exploration's metadata, archive state, or move it to a different collection.

  When `collection_id` changes, the caller must have write perms on the destination collection
  (or the root collection when `collection_id` is nil). Source perms are enforced by
  `api/write-check` against the exploration itself via `:perms/use-parent-collection-perms`."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]
   _query-params
   updates :- UpdateExploration]
  (let [existing (get-exploration-or-404 id)
        updates' (api/updates-with-archived-directly existing updates)]
    (api/write-check existing)
    (check-destination-collection-perms! existing updates')
    (t2/with-transaction [_]
      (when (seq updates')
        (t2/update! :model/Exploration id updates')))
    (let [updated (t2/select-one :model/Exploration :id id)]
      (when (seq updates')
        (events/publish-event! :event/exploration-update
                               {:object updated :user-id api/*current-user-id*}))
      (hydrate-exploration updated))))

(api.macros/defendpoint :delete "/:id" :- :nil
  "Hard-delete an exploration. Soft delete is `PUT /api/exploration/:id {archived: true}`.

  Cascades to every `exploration_thread` via the on-delete-cascade FKs configured in the
  explorations migration."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]]
  (let [existing (get-exploration-or-404 id)]
    (api/write-check existing)
    (t2/delete! :model/Exploration :id id))
  nil)

;;; ----------------------------------------- routes -----------------------------------------

(def ^{:arglists '([request respond raise])} routes
  "`/api/exploration/` routes."
  (api.macros/ns-handler *ns* +auth))
