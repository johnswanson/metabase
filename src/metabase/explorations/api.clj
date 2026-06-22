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
   [metabase.queries.core :as queries]
   [metabase.query-processor.middleware.cache.impl :as cache.impl]
   [metabase.query-processor.pipeline :as qp.pipeline]
   [metabase.query-processor.streaming :as qp.streaming]
   [metabase.request.core :as request]
   [metabase.util.malli.registry :as mr]
   [metabase.util.malli.schema :as ms]
   [toucan2.core :as t2])
  (:import
   (java.io ByteArrayInputStream)))

(set! *warn-on-reflection* true)

;;; ----------------------------------------- helpers -----------------------------------------

(defn- get-exploration-or-404 [id]
  (api/check-404 (t2/select-one :model/Exploration :id id)))

(defn- get-exploration-query-or-404
  "Fetch an `ExplorationQuery` by id and read-check it. The model's `can-read?` delegates up
  through `ExplorationThread` to the parent `Exploration`."
  [query-id]
  (api/read-check (api/check-404 (t2/select-one :model/ExplorationQuery :id query-id))))

(defn- thread-to-restart
  "The thread a restart re-runs. The exploration UI is single-thread for now, so this is just the
  exploration's (latest) thread; a future multi-thread UI will pass the thread id explicitly."
  [exploration-id]
  (t2/select-one :model/ExplorationThread
                 :exploration_id exploration-id
                 {:order-by [[:position :desc] [:id :desc]]}))

(defn- reset-thread-for-rerun!
  "Clear a thread's prior run so the background worker re-plans and re-executes it: drop its
  ExplorationQuery rows and reset the plan/terminal state, re-stamping `started_at`."
  [thread-id]
  (t2/delete! :model/ExplorationQuery :exploration_thread_id thread-id)
  (t2/update! :model/ExplorationThread thread-id
              {:started_at            (t/offset-date-time)
               :query_plan_started_at nil
               :query_plan_transcript nil
               :completed_at          nil
               :canceled_at           nil}))

(def ^:private query-summary-columns
  "Column projection for `::ExplorationQuerySummary` rows — excludes `dataset_query` and the
  result blob, joins both interestingness scores from `exploration_query_result`."
  [:exploration_query.id :exploration_query.exploration_thread_id
   :exploration_query.card_id :exploration_query.segment_id
   :exploration_query.dimension_id :exploration_query.query_type
   :exploration_query.display :exploration_query.name :exploration_query.position
   :exploration_query.status :exploration_query.error_message
   :exploration_query.user_interestingness
   :exploration_query.entity_id
   [:exploration_query_result.interestingness_score            :interestingness_score]
   [:exploration_query_result.contextual_interestingness_score :contextual_interestingness_score]])

(defn- query-summary
  "Fetch a single `::ExplorationQuerySummary` row by `exploration_query.id`."
  [query-id]
  (t2/select-one (into [:model/ExplorationQuery] query-summary-columns)
                 {:left-join [:exploration_query_result
                              [:= :exploration_query_result.exploration_query_id :exploration_query.id]]
                  :where     [:= :exploration_query.id query-id]}))

(defn- get-thread-or-404
  "Fetch the thread and its parent exploration, or 404."
  [thread-id]
  (api/check-404 (t2/select-one :model/ExplorationThread :id thread-id)))

(defn- write-check-thread [thread-id]
  (let [thread (get-thread-or-404 thread-id)]
    (api/write-check (get-exploration-or-404 (:exploration_id thread)))
    thread))

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

(defn- insert-thread-groups!
  "Persist the FE's Research-plan groups verbatim — one `ExplorationThreadGroup` row per
   group, in payload order. Each group keeps its own `:metrics`/`:dimensions` selection."
  [thread-id groups]
  (when (seq groups)
    (t2/insert! :model/ExplorationThreadGroup
                (positional-rows thread-id
                                 (map #(select-keys % [:type :metrics :dimensions]) groups)))))

(defn- insert-thread-timelines! [thread-id timeline-ids]
  (when (seq timeline-ids)
    (t2/insert! :model/ExplorationThreadTimeline
                (positional-rows thread-id
                                 (map (fn [tl-id] {:timeline_id tl-id}) timeline-ids)))))

;;; ----------------------------------------- schemas -----------------------------------------

(mr/def ::HydratedThread
  "Schema for an Exploration thread."
  [:map
   [:id             ms/PositiveInt]
   [:exploration_id ms/PositiveInt]
   [:prompt         {:optional true} [:maybe :string]]
   [:position       ms/IntGreaterThanOrEqualToZero]
   [:started_at     {:optional true} [:maybe :any]]
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

(mr/def ::ExplorationSummary
  "Lightweight row for the `GET /mine` list. No threads — just the metadata needed to render a list
  entry, plus `current_user_last_touched_at`, the timestamp the list is sorted by (the caller's own
  most-recent touch of this exploration, composed across the exploration's revisions and its
  creation)."
  [:map
   [:id                           ms/PositiveInt]
   [:name                         ms/NonBlankString]
   [:description                  {:optional true} [:maybe :string]]
   [:creator_id                   ms/PositiveInt]
   [:creator                      {:optional true}
    [:maybe [:map
             [:id         {:optional true} ms/PositiveInt]
             [:email      {:optional true} ms/NonBlankString]
             [:first_name {:optional true} [:maybe :string]]
             [:last_name  {:optional true} [:maybe :string]]]]]
   [:collection_id                {:optional true} [:maybe ms/PositiveInt]]
   [:collection                   {:optional true}
    [:maybe [:map
             [:id   ms/PositiveInt]
             [:name ms/NonBlankString]]]]
   [:archived                     {:optional true} :boolean]
   [:created_at                   ms/TemporalInstant]
   [:updated_at                   ms/TemporalInstant]
   [:current_user_last_touched_at ms/TemporalInstant]])

(mr/def ::MineResponse
  "Paginated envelope for `GET /mine`, mirroring the collection-items index shape."
  [:map
   [:total  ms/IntGreaterThanOrEqualToZero]
   [:limit  [:maybe ms/IntGreaterThanOrEqualToZero]]
   [:offset [:maybe ms/IntGreaterThanOrEqualToZero]]
   [:data   [:sequential ::ExplorationSummary]]])

(mr/def ::ExplorationQuerySummary
  "A planned query row in API responses. The result blob and `dataset_query` aren't asserted
  here; interestingness scores and timeline scoring are hydrated in later slices."
  [:map
   [:id                    ms/PositiveInt]
   [:exploration_thread_id ms/PositiveInt]
   [:card_id               ms/PositiveInt]
   [:segment_id            {:optional true} [:maybe ms/PositiveInt]]
   [:dimension_id          [:maybe :string]]
   [:query_type            :string]
   [:display               {:optional true} [:maybe :string]]
   [:name                  {:optional true} [:maybe :string]]
   [:position              ms/IntGreaterThanOrEqualToZero]
   [:status                :string]
   [:error_message         {:optional true} [:maybe :string]]
   [:user_interestingness  {:optional true} [:maybe [:enum 0 1 2]]]
   [:entity_id             {:optional true} [:maybe :string]]
   [:interestingness_score            {:optional true} [:maybe number?]]
   [:contextual_interestingness_score {:optional true} [:maybe number?]]
   [:timeline_interestingness         {:optional true} [:maybe [:sequential
                                                                [:map
                                                                 [:timeline_id           ms/PositiveInt]
                                                                 [:interestingness_score {:optional true} [:maybe number?]]]]]]])

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

(mr/def ::CanceledThread
  "Schema for the cancel endpoint response — just the state-bearing fields the FE needs to
  reflect the cancellation. EQ status changes are picked up via the existing `/queries` poll."
  [:map
   [:id           ms/PositiveInt]
   [:canceled_at  [:maybe :any]]
   [:completed_at [:maybe :any]]])

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

(def ^:private GroupSelection
  "One Research-plan area on the FE — either a metric area (one primary metric + chosen dimensions)
   or a dimension area (the dimension's group + referencing metrics). Persisted verbatim as one
   `ExplorationThreadGroup` row; the planners cross this group's metrics with this group's
   dimensions only."
  [:map
   [:type       {:optional true} [:maybe [:enum "metric" "dimension"]]]
   [:metrics    {:optional true} [:maybe [:sequential MetricSelection]]]
   [:dimensions {:optional true} [:maybe [:sequential DimensionSelection]]]])

(def ^:private CreateExploration
  "Body schema for `POST /api/exploration`. The FE sends one entry per Research-plan group
   (`:groups`), each persisted verbatim."
  [:map
   [:name          expl.model/ExplorationName]
   [:description   {:optional true} [:maybe :string]]
   [:prompt        {:optional true} [:maybe :string]]
   [:collection_id {:optional true} [:maybe ms/PositiveInt]]
   [:groups       {:optional true} [:maybe [:sequential GroupSelection]]]
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

;;; ----------------------------------------- /dimensions schemas -----------------------------------------

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

(api.macros/defendpoint :get "/dimensions" :- ::DimensionsResponse
  "Hydrated metrics plus a deduplicated dimension list, for the Exploration data modal.

  Optional `q` filters case-insensitively across metric name and dimension display-name."
  [_route-params
   {:keys [q]} :- [:maybe [:map [:q {:optional true} [:maybe ms/NonBlankString]]]]]
  (explorations/exploration-data {:q q}))

(api.macros/defendpoint :post "/" :- ::HydratedExploration
  "Create a new exploration with a single thread and stamp the thread as started."
  [_route-params
   _query-params
   {:keys [name description prompt collection_id groups timeline_ids]} :- CreateExploration]
  (api/create-check :model/Exploration {:collection_id collection_id})
  (t2/with-transaction [_]
    (let [exploration (first (t2/insert-returning-instances! :model/Exploration
                                                             {:name          name
                                                              :description   description
                                                              :collection_id collection_id
                                                              :creator_id    api/*current-user-id*}))
          thread      (first (t2/insert-returning-instances! :model/ExplorationThread
                                                             {:exploration_id (:id exploration)
                                                              :prompt         prompt
                                                              :position       0}))]
      (insert-thread-groups! (:id thread) groups)
      (insert-thread-timelines! (:id thread) timeline_ids)
      (t2/update! :model/ExplorationThread (:id thread) {:started_at (t/offset-date-time)})
      (let [persisted (t2/select-one :model/Exploration :id (:id exploration))]
        (events/publish-event! :event/exploration-create
                               {:object persisted :user-id api/*current-user-id*})
        (hydrate-exploration persisted)))))

(defn- my-explorations-honeysql
  "HoneySQL for the explorations `user-id` created or edited, ordered by that user's most-recent
  touch (descending). \"Touch\" is the union of the user's `Exploration` revisions and
  `exploration.created_at` for explorations the user created."
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
  "Explorations the current user created or edited, most-recently-touched first, paginated."
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

(api.macros/defendpoint :get "/:id/queries" :- [:sequential ::ExplorationQuerySummary]
  "Lightweight list of an exploration's planned queries — excludes `dataset_query` and the result
  blob. The frontend polls this while the background planning worker materializes rows."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]]
  (api/read-check (get-exploration-or-404 id))
  (t2/hydrate
   (t2/select (into [:model/ExplorationQuery] query-summary-columns)
              {:left-join [:exploration_thread
                           [:= :exploration_query.exploration_thread_id :exploration_thread.id]
                           :exploration_query_result
                           [:= :exploration_query_result.exploration_query_id :exploration_query.id]]
               :where     [:= :exploration_thread.exploration_id id]
               :order-by  [[:exploration_query.position :asc] [:exploration_query.id :asc]]})
   :timeline_interestingness))

(api.macros/defendpoint :post "/:id/restart" :- ::HydratedExploration
  "Re-run an exploration's analysis in place."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]]
  (let [exploration (get-exploration-or-404 id)
        _           (api/write-check exploration)
        thread      (api/check-404 (thread-to-restart id))]
    (t2/with-transaction [_]
      (reset-thread-for-rerun! (:id thread))
      (let [updated (t2/select-one :model/Exploration :id id)]
        (events/publish-event! :event/exploration-update
                               {:object updated :user-id api/*current-user-id*})
        (hydrate-exploration updated)))))

(defn- stream-stored-result
  "Replay a worker-serialized QP result (gzipped+nippy bytes from `:model/StoredResult.result_data`)
  through the streaming pipeline so the response is shaped like a normal `/api/dataset` response.
  Reuses
  `cache.impl/with-reducible-deserialized-results` — the same machinery the cache middleware
  uses to replay cached results."
  [export-format ^bytes result-bytes]
  (qp.streaming/streaming-response [rff export-format]
    (cache.impl/with-reducible-deserialized-results
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

(api.macros/defendpoint :put "/query/:id/interesting" :- ::ExplorationQuerySummary
  "Set the owner's interestingness rating on an exploration query.
  `user_interestingness` is `0` (not interesting), `1` (hmm), or `2` (interesting)."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]
   _query-params
   {:keys [user_interestingness]} :- [:map [:user_interestingness [:enum 0 1 2]]]]
  (api/write-check (api/check-404 (t2/select-one :model/ExplorationQuery :id id)))
  (t2/update! :model/ExplorationQuery id {:user_interestingness user_interestingness})
  (query-summary id))

(api.macros/defendpoint :delete "/query/:id/interesting" :- ::ExplorationQuerySummary
  "Clear the owner's interestingness rating on an exploration query."
  [{:keys [id]} :- [:map [:id ms/PositiveInt]]]
  (api/write-check (api/check-404 (t2/select-one :model/ExplorationQuery :id id)))
  (t2/update! :model/ExplorationQuery id {:user_interestingness nil})
  (query-summary id))

(api.macros/defendpoint :post "/thread/:thread-id/cancel" :- ::CanceledThread
  "Cancel an in-flight exploration thread. Stamps `canceled_at` and `completed_at` on the thread,
  and bulk-flips any still-`pending` ExplorationQuery rows to `canceled`. In-flight queries
  currently mid-QP-execution are left to run to natural completion — their result rows are
  orphaned but harmless (timeline scoring and AI Summary both skip canceled threads).

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

(api.macros/defendpoint :put "/:id" :- ::HydratedExploration
  "Update an exploration's metadata, archive state, or move it to a different collection."
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
