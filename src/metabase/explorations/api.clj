(ns metabase.explorations.api
  "`/api/exploration` routes."
  (:require
   [java-time.api :as t]
   [metabase.api.common :as api]
   [metabase.api.macros :as api.macros]
   [metabase.api.routes.common :refer [+auth]]
   [metabase.app-db.core :as mdb]
   [metabase.collections.models.collection :as collection]
   [metabase.events.core :as events]
   [metabase.explorations.core :as explorations]
   [metabase.explorations.models.exploration :as expl.model]
   [metabase.explorations.models.exploration-query-result :as eqr]
   [metabase.explorations.queues :as explorations.queues]
   [metabase.queries.core :as queries]
   [metabase.query-processor.core :as qp]
   [metabase.query-processor.pipeline :as qp.pipeline]
   [metabase.query-processor.streaming :as qp.streaming]
   [metabase.request.core :as request]
   [metabase.util.i18n :refer [tru]]
   [metabase.util.malli.registry :as mr]
   [metabase.util.malli.schema :as ms]
   [toucan2.core :as t2])
  (:import
   (java.io ByteArrayInputStream)))

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
  (t2/hydrate exploration :creator :can_write :collection [:threads :timelines]))

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

(defn- insert-thread-timelines!
  "Attach `timeline-ids` to the thread, in payload order. Deduped (`distinct`, keeping first
   occurrence) — a repeated id would otherwise violate the table's unique
   `(exploration_thread_id, timeline_id)` constraint and 500 the create."
  [thread-id timeline-ids]
  (when (seq timeline-ids)
    (t2/insert! :model/ExplorationThreadTimeline
                (positional-rows thread-id
                                 (map (fn [tl-id] {:timeline_id tl-id}) (distinct timeline-ids))))))

(defn- reset-thread-for-rerun!
  "CAS-reset a *terminal* thread (`completed_at` set — natural completion, terminal failure, or
  cancel) back to the freshly-started state a new plan run expects: `started_at` set, every other
  lifecycle timestamp NULL, and zero `exploration_query` rows. On success it enqueues a fresh
  planning message (`explorations.queues/start-thread!`) inside the same transaction, so planning
  re-runs iff the reset committed. Returns true when the reset applied; false when the guarded
  UPDATE matched no row.

  The guard refuses while the thread is still in flight: not yet terminal, or a query worker
  still holds a `running` row (possible on a canceled thread, whose in-flight queries run to
  natural completion). A restart racing in-flight work could otherwise strand query rows a
  still-running planner inserts after the reset, or let an in-flight query worker's completion
  CAS stamp the freshly-reset thread."
  [thread-id]
  (t2/with-transaction [_conn]
    (when (pos? (t2/query-one
                 {:update :exploration_thread
                  :set    {:started_at            (t/offset-date-time)
                           :query_plan_started_at nil
                           :query_plan_transcript nil
                           :analysis_started_at   nil
                           :completed_at          nil
                           :canceled_at           nil}
                  :where  [:and
                           [:= :id thread-id]
                           [:not= :completed_at nil]
                           [:not-exists {:select [1]
                                         :from   [:exploration_query]
                                         :where  [:and
                                                  [:= :exploration_thread_id thread-id]
                                                  [:= :status "running"]]}]]}))
      (t2/delete! :model/ExplorationQuery :exploration_thread_id thread-id)
      ;; Enqueue planning inside the reset transaction so the plan message publishes iff the reset
      ;; commits (:queue/exploration-plan is :transactional :require).
      (explorations.queues/start-thread! thread-id)
      true)))

;;; ----------------------------------------- schemas -----------------------------------------

(mr/def ::HydratedThread
  "Schema for an Exploration thread."
  [:map
   [:id             ms/PositiveInt]
   [:exploration_id ms/PositiveInt]
   [:prompt         {:optional true} [:maybe :string]]
   [:position       ms/IntGreaterThanOrEqualToZero]
   [:started_at     {:optional true} [:maybe :any]]
   [:canceled_at    {:optional true} [:maybe :any]]
   [:completed_at   {:optional true} [:maybe :any]]
   [:timelines      {:optional true}
    [:maybe [:sequential
             [:map
              [:timeline_id ms/PositiveInt]
              [:position    {:optional true} ms/IntGreaterThanOrEqualToZero]
              [:timeline    {:optional true} [:maybe :map]]]]]]])

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

(mr/def ::ExplorationQuerySummary
  "Schema for a query row in API responses. The result blob and `dataset_query` aren't
   asserted here; `interestingness_score`, `contextual_interestingness_score`, and
   `row_count` are left-joined (in `/:id/queries`) or batched-hydrated (in `/:id`) — the
   scores from `exploration_query_result`, `row_count` from the linked `stored_result` —
   and may be nil for pending/errored queries or — for the contextual score — when the LLM
   is unconfigured or the thread had no prompt."
  [:map
   [:id                               ms/PositiveInt]
   [:exploration_thread_id            ms/PositiveInt]
   [:card_id                          ms/PositiveInt]
   [:segment_id                       {:optional true} [:maybe ms/PositiveInt]]
   [:segment_name                     {:optional true} [:maybe :string]]
   [:dimension_id                     [:maybe :string]]
   [:dimension_name                   {:optional true} :string]
   [:query_type                       :string]
   [:display                          {:optional true} [:maybe :string]]
   [:name                             {:optional true} [:maybe :string]]
   [:position                         ms/IntGreaterThanOrEqualToZero]
   [:status                           :string]
   [:error_message                    {:optional true} [:maybe :string]]
   [:started_at                       {:optional true} [:maybe :any]]
   [:finished_at                      {:optional true} [:maybe :any]]
   [:entity_id                        {:optional true} [:maybe :string]]
   [:interestingness_score            {:optional true} [:maybe number?]]
   [:contextual_interestingness_score {:optional true} [:maybe number?]]
   [:row_count                        {:optional true} [:maybe ms/IntGreaterThanOrEqualToZero]]])

(mr/def ::ExplorationQueryStreamResponse
  "Schema for `GET /query/:id`. On success the body is a streamed dataset (api/csv/json/xlsx),
   so we describe it as `:any`. On a not-yet-done query we return a 409 with a status payload."
  [:or
   :any
   [:map
    [:status [:= 409]]
    [:body   [:map
              [:id            ms/PositiveInt]
              [:status        :string]
              [:error_message {:optional true} [:maybe :string]]
              [:started_at    {:optional true} [:maybe :any]]
              [:finished_at   {:optional true} [:maybe :any]]]]]])

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
   area), each persisted verbatim. `:timeline_ids` is thread-scoped (timelines aren't part of
   any metric×dimension cross-product) and lives at the top level, not inside a block."
  [:map
   [:name          expl.model/ExplorationName]
   [:description   {:optional true} [:maybe :string]]
   [:prompt        {:optional true} [:maybe :string]]
   [:collection_id {:optional true} [:maybe ms/PositiveInt]]
   [:blocks        {:optional true} [:maybe [:sequential BlockSelection]]]
   [:timeline_ids  {:optional true} [:maybe [:sequential ms/PositiveInt]]]])

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
  "Create a new exploration with a single thread, persist the user's selected metrics, dimensions,
  and timelines, and stamp the thread as started. Actual planning is async; this endpoint returns
  immediately with an empty queries list. Clients should poll `GET /:id/queries` until rows appear.

  Accepts the per-area `:blocks` payload (one entry per Research-plan block), persisted
  verbatim, plus a thread-scoped `:timeline_ids`."
  [_route-params
   _query-params
   {:keys [name description prompt collection_id blocks timeline_ids]} :- CreateExploration]
  (api/create-check :model/Exploration {:collection_id collection_id})
  ;; Block metric-card and timeline references are persisted verbatim and read back unfiltered
  ;; (planning context, thread hydration), so attach time is the
  ;; permission boundary: every referenced id must exist (404) and be readable (403) by the creator.
  (doseq [card-id (distinct (mapcat #(map :card_id (:metrics %)) blocks))]
    (api/read-check :model/Card card-id))
  (doseq [timeline-id (distinct timeline_ids)]
    (api/read-check :model/Timeline timeline-id))
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
                ;; including the dependent block/timeline rows below, commits atomically.
                ;; The plan listener therefore can never observe (or plan) a half-built thread.
                thread      (first (t2/insert-returning-instances! :model/ExplorationThread
                                                                   {:exploration_id (:id exploration)
                                                                    :prompt         prompt
                                                                    :position       0
                                                                    :started_at     (t/offset-date-time)}))
                tid         (:id thread)]
            (insert-blocks! tid blocks)
            (insert-thread-timelines! tid timeline_ids)
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

(def ^:private query-summary-columns
  "Column projection for `::ExplorationQuerySummary` rows — excludes `dataset_query` and the
  result blob, joins both interestingness scores from `exploration_query_result` and the
  snapshot `row_count` from `stored_result` (reached through the EQR FK — callers must
  left-join both tables)."
  [:exploration_query.id :exploration_query.exploration_thread_id
   :exploration_query.card_id :exploration_query.segment_id
   :exploration_query.dimension_id :exploration_query.query_type
   :exploration_query.name :exploration_query.position
   :exploration_query.status :exploration_query.error_message
   :exploration_query.started_at :exploration_query.finished_at
   :exploration_query.entity_id
   [:exploration_query_result.interestingness_score            :interestingness_score]
   [:exploration_query_result.contextual_interestingness_score :contextual_interestingness_score]
   [:stored_result.row_count                                    :row_count]])

(defn- get-thread-or-404
  "Fetch the thread, or 404."
  [thread-id]
  (api/check-404 (t2/select-one :model/ExplorationThread :id thread-id)))

(defn- write-check-thread [thread-id]
  (let [thread (get-thread-or-404 thread-id)]
    (api/write-check (get-exploration-or-404 (:exploration_id thread)))
    thread))

(api.macros/defendpoint :post "/thread/:thread-id/restart" :- ::HydratedExploration
  "Re-run one exploration thread in place, keeping its selections: drops the thread's materialized
  queries and clears the terminal-state gates so the background planner re-claims it. Returns the
  parent exploration. Only a terminal thread (completed, failed, or canceled) can restart; while
  planning, execution, or analysis is still in flight this returns a 409 — cancel the thread
  first, then restart.

  No `:event/exploration-update` is published: nothing on the Exploration row changes, so there
  is no revision to record (the revision push skips unchanged objects)."
  [{:keys [thread-id]} :- [:map [:thread-id ms/PositiveInt]]]
  (let [thread      (get-thread-or-404 thread-id)
        exploration (api/write-check (get-exploration-or-404 (:exploration_id thread)))]
    (when-not (reset-thread-for-rerun! thread-id)
      (throw (ex-info (tru "Exploration is still running; cancel it before restarting.")
                      {:status-code 409})))
    (hydrate-exploration exploration)))

(mr/def ::CanceledThread
  "Schema for the cancel endpoint response — just the state-bearing fields the FE needs to
  reflect the cancellation. EQ status changes are picked up via the existing `/queries` poll."
  [:map
   [:id           ms/PositiveInt]
   [:canceled_at  [:maybe :any]]
   [:completed_at [:maybe :any]]])

(api.macros/defendpoint :post "/thread/:thread-id/cancel" :- ::CanceledThread
  "Cancel an in-flight exploration thread. Stamps `canceled_at` and `completed_at` on the thread,
  and bulk-flips any still-`pending` ExplorationQuery rows to `canceled`. In-flight queries
  currently mid-QP-execution are left to run to natural completion — their result rows are
  orphaned but harmless (timeline scoring skips canceled threads).

  Idempotent: a thread with `completed_at IS NOT NULL` (already terminal — natural completion or
  prior cancel) returns 200 with its existing state. Authorization is the same write check as
  other thread-mutating endpoints."
  [{:keys [thread-id]} :- [:map [:thread-id ms/PositiveInt]]]
  (write-check-thread thread-id)
  (let [now (t/offset-date-time)]
    (t2/with-transaction [_conn]
      ;; CAS gate on `completed_at IS NULL` makes both already-canceled and already-completed
      ;; threads safe no-ops. When this UPDATE matches 0 rows, the thread is already terminal.
      (t2/update! :model/ExplorationThread
                  :id           thread-id
                  :completed_at nil
                  {:canceled_at now
                   :completed_at now})
      ;; Bulk-flip pending → canceled. SKIP LOCKED on Postgres/MySQL skips the row currently
      ;; held by an in-flight QP worker so this API call doesn't block on QP duration; that row
      ;; will commit as `done` (or `error`) naturally. H2 has only one worker (see worker-count
      ;; in the runner) so SKIP LOCKED is unnecessary and unsupported.
      ;;
      ;; Done as a select-then-update rather than `WHERE id IN (subquery on the same table)`:
      ;; MySQL/MariaDB reject updating a table referenced by a subquery in the same statement
      ;; (error 1093). The selected rows stay locked until the surrounding transaction commits,
      ;; so the SKIP LOCKED semantics are preserved.
      (let [pending-ids (map :id
                             (t2/query
                              (cond-> {:select [:id]
                                       :from   [:exploration_query]
                                       :where  [:and
                                                [:= :exploration_thread_id thread-id]
                                                [:= :status "pending"]]}
                                (not= :h2 (mdb/db-type)) (assoc :for [:update :skip-locked]))))]
        (when (seq pending-ids)
          (t2/query
           {:update (t2/table-name :model/ExplorationQuery)
            :set    {:status "canceled"}
            :where  [:in :id pending-ids]})))))
  (t2/select-one [:model/ExplorationThread :id :canceled_at :completed_at] :id thread-id))

(api.macros/defendpoint :get "/:id/queries" :- [:sequential ::ExplorationQuerySummary]
  "Lightweight list of queries for an exploration. Excludes `dataset_query` and the result blob —
  intended for the frontend to poll while pending queries finish. The `interestingness_score`
  column is left-joined from `exploration_query_result` so clients can rank/highlight without a
  second roundtrip; pending or errored queries get `nil`."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]]
  (api/read-check (get-exploration-or-404 id))
  (t2/hydrate
   (t2/select (into [:model/ExplorationQuery] query-summary-columns)
              {:left-join [:exploration_thread
                           [:= :exploration_query.exploration_thread_id :exploration_thread.id]
                           :exploration_query_result
                           [:= :exploration_query_result.exploration_query_id :exploration_query.id]
                           :stored_result
                           [:= :stored_result.id :exploration_query_result.stored_result_id]]
               :where     [:= :exploration_thread.exploration_id id]
               :order-by  [[:exploration_query.position :asc]
                           [:exploration_query.id :asc]]})
   :segment_name))

(defn- get-exploration-query-or-404
  "Fetch an `ExplorationQuery` by id and read-check it. The model's `can-read?` delegates up
  through `ExplorationThread` to the parent `Exploration`."
  [query-id]
  (api/read-check (api/check-404 (t2/select-one :model/ExplorationQuery :id query-id))))

(defn- stream-stored-result
  "Replay a worker-serialized QP result (gzipped+nippy bytes from `:model/StoredResult.result_data`)
  through the streaming pipeline so the response is shaped like a normal `/api/dataset` response.
  Reuses
  `qp/with-reducible-deserialized-results` — the same machinery the cache middleware
  uses to replay cached results."
  [export-format ^bytes result-bytes]
  (qp.streaming/streaming-response [rff export-format]
    (qp/with-reducible-deserialized-results
      [[qp-result _] (ByteArrayInputStream. result-bytes)]
      (when qp-result
        (let [data (:data qp-result)]
          (qp.pipeline/*reduce* rff
                                (dissoc data :rows)
                                (or (:rows data) [])))))))

(api.macros/defendpoint :get "/query/:id" :- ::ExplorationQueryStreamResponse
  "Stream the result of a single completed exploration query. The optional `format` query param
  is one of `api`, `json`, `csv`, `xlsx` (default `api`). When the underlying query is still
  pending or has errored, returns a 409 with status info instead of streaming."
  [{:keys [id]}     :- [:map [:id ms/PositiveInt]]
   {:keys [format]} :- [:map
                        [:format {:default :api}
                         [:enum {:decode/api keyword} :api :csv :json :xlsx]]]]
  (let [q (get-exploration-query-or-404 id)]
    (case (:status q)
      "done"
      (let [sr (api/check-404 (eqr/stored-results id))]
        ;; The cached `result_data` was produced under the creator's lens, so a non-creator viewer
        ;; might otherwise see rows the QP would have filtered out for them. Gate against the
        ;; creator's stored data-access token (sandbox/impersonation/routing) + basic data perms.
        (when-not (= api/*current-user-id* (:creator_id sr))
          (queries/assert-can-view-cached-result! sr))
        (stream-stored-result format (:result_data sr)))

      ;; Pending / errored: no blob exists yet and the response is status-only (no rows, no
      ;; derived text), so it carries no data to leak — it rides the exploration's collection
      ;; perms (already enforced by `get-exploration-query-or-404`'s read-check), like seeing a
      ;; dashboard card that's still loading.
      {:status 409
       :body   (select-keys q [:id :status :error_message :started_at :finished_at])})))

;;; ----------------------------------------- routes -----------------------------------------

(def ^{:arglists '([request respond raise])} routes
  "`/api/exploration/` routes."
  (api.macros/ns-handler *ns* +auth))
