(ns metabase.explorations.runner
  "The work an exploration does in the background, as two plain functions:

    [[plan-thread!]]  — ask the LLM which charts to build, and materialize them as
                        `:model/ExplorationQuery` rows
    [[run-query!]]    — run one query's MBQL through the QP and store the result

  Each is idempotent for calling from MQ."
  (:require
   [medley.core :as m]
   [metabase.analytics-interface.core :as analytics.interface]
   [metabase.analytics.core :as analytics]
   [metabase.explorations.interestingness :as explorations.interestingness]
   [metabase.explorations.query-plan :as explorations.query-plan]
   [metabase.explorations.query-plan.context :as qp.context]
   [metabase.explorations.query-plan.variants :as qp.variants]
   [metabase.interestingness.core :as interestingness]
   [metabase.lib.core :as lib]
   [metabase.permissions.core :as perms]
   [metabase.query-permissions.core :as query-perms]
   [metabase.query-processor.core :as qp]
   [metabase.request.core :as request]
   [metabase.util.log :as log]
   [toucan2.core :as t2])
  (:import
   (java.time Duration OffsetDateTime)))

(set! *warn-on-reflection* true)

;;; ------------------------------------- Prometheus metrics -------------------------------------

(defn- pending-query-depth
  "Number of `exploration_query` rows currently awaiting execution."
  []
  (t2/count :model/ExplorationQuery :status "pending"))

(defn- oldest-pending-age-seconds
  "Age in seconds of the oldest still-pending `exploration_query`, or 0 when the queue is empty.
  Computed as `now - min(created_at)` so it keeps climbing while the runner is stalled."
  []
  (if-let [oldest (t2/select-one-fn :created_at :model/ExplorationQuery
                                    {:where    [:= :status "pending"]
                                     :order-by [[:created_at :asc]]
                                     :limit    1})]
    (max 0 (.toSeconds (Duration/between ^OffsetDateTime oldest (OffsetDateTime/now))))
    0))

(defmethod analytics/pull-collector ::queue [_]
  {:min-interval-s 30
   :f (fn []
        (analytics.interface/set-gauge! :metabase-explorations/pending-queue-depth
                                        (pending-query-depth))
        (analytics.interface/set-gauge! :metabase-explorations/oldest-pending-age-seconds
                                        (oldest-pending-age-seconds)))})

(defn- record-query-outcome!
  "Bump the `queries-processed` counter for a terminal query `status` (\"done\" / \"error\")."
  [status]
  (analytics.interface/inc! :metabase-explorations/queries-processed {:status status}))

(defn- runnable-query
  "Load the `ExplorationQuery` `query-id` if it is still work to do, else nil."
  [query-id]
  (t2/select-one :model/ExplorationQuery
                 {:select [:eq.*]
                  :from   [[:exploration_query :eq]]
                  :join   [[:exploration_thread :et] [:= :et.id :eq.exploration_thread_id]]
                  :where  [:and
                           [:= :eq.id query-id]
                           [:= :eq.status "pending"]
                           [:= :et.canceled_at nil]]}))

(defn- serialize-result
  "Run `cache.impl/do-with-serialization` against a single QP result, returning the gzipped+nippy
  byte array.

  Mirrors the prep step the QP's own result-cache middleware does (see
  `metabase.query-processor.middleware.cache/add-object-to-cache!`):
  `:json_query` and `:preprocessed_query` are passed through
  `lib/prepare-for-serialization` so the metadata provider — a record
  holding caching atoms that Nippy can't freeze — is stripped before
  serialization. Without this prep, Nippy chokes on the `Atom` inside the
  mp the moment we hand it a qp-result whose input query is a pMBQL value
  with `:lib/metadata` still attached."
  ^bytes [qp-result]
  (qp/do-with-serialization
   (fn [in result-fn]
     (in (cond-> qp-result
           (map? qp-result) (-> (m/update-existing :json_query lib/prepare-for-serialization)
                                (m/update-existing :preprocessed_query lib/prepare-for-serialization))))
     (result-fn))))

(defn- finalize-row!
  "If `row` carries a nil `:dataset_query` (planner deferred the MBQL build),
  resolve the per-row context, invoke `qp.variants/dataset-query` and
  `qp.variants/query-name` for the row's variant, persist both back onto the
  row, and return the row with both fields populated. Throws when the
  context can't be built or when the variant's `dataset-query` returns nil
  (e.g. top-K discovery returned no rows) — the caller's catch handler
  records it as a row-level error."
  [row]
  (if (:dataset_query row)
    row
    (let [ctx (qp.context/build-row-context row)]
      (when-not ctx
        (throw (ex-info "Could not build context for row"
                        {:row-id (:id row)})))
      (let [variant (:query_type row)
            dq      (qp.variants/dataset-query variant ctx)
            nm      (qp.variants/query-name variant ctx)]
        (when (nil? dq)
          (throw (ex-info "Could not build dataset_query for row (discovery returned no values?)"
                          {:row-id (:id row) :variant variant})))
        (t2/update! :model/ExplorationQuery (:id row)
                    {:dataset_query dq :name nm})
        (assoc row :dataset_query dq :name nm)))))

(defn- safe-chart-config
  "Best-effort `qp-result->chart-config`. Returns nil on failure (>2 cols,
  no numeric measure, unexpected shape, or any throw)."
  [exploration-query qp-result]
  (try
    (explorations.interestingness/qp-result->chart-config exploration-query qp-result)
    (catch Throwable e
      (log/warnf e "Failed to build chart-config for ExplorationQuery %d" (:id exploration-query))
      nil)))

(defn- safe-deep-stats
  "Best-effort deep `compute-chart-stats`. Returns nil on failure. Stored on
  the result row so summarization, chart-detail UIs, and any other consumer
  read the cached stats instead of re-running the pipeline against the
  serialized result blob."
  [exploration-query chart-config]
  (when chart-config
    (try
      (interestingness/compute-chart-stats chart-config {:deep? true})
      (catch Throwable e
        (log/warnf e "Failed to compute chart stats for ExplorationQuery %d" (:id exploration-query))
        nil))))

(defn- safe-score
  "Best-effort interestingness score. Reuses the pre-computed `stats` so we
  don't run the stats pipeline twice. Logs and returns nil on any failure so
  the worker still persists the result row — a scoring bug must never flip a
  successful query to errored.

  At `debug` level, logs the full statistical breakdown (non-degeneracy / signal /
  structure sub-scores + chart-type) so the blended score isn't a black box when
  diagnosing why a chart did or didn't earn the \"potentially interesting\" marker."
  [exploration-query chart-config stats]
  (try
    (when (and chart-config stats)
      (let [breakdown (interestingness/chart-interestingness chart-config stats)]
        (log/debugf "Statistical interestingness for ExplorationQuery %d (thread %d): %s"
                    (:id exploration-query) (:exploration_thread_id exploration-query) (pr-str breakdown))
        (:score breakdown)))
    (catch Throwable e
      (log/warnf e "Failed to compute interestingness for ExplorationQuery %d"
                 (:id exploration-query))
      nil)))

(defn- exploration-creator-id
  "Walk EQ → ExplorationThread → Exploration.creator_id for stamping onto the stored_result."
  [exploration-query]
  (t2/select-one-fn :creator_id :model/Exploration
                    {:join  [:exploration_thread
                             [:= :exploration_thread.exploration_id :exploration.id]]
                     :where [:= :exploration_thread.id (:exploration_thread_id exploration-query)]}))

(defn- exploration-id
  "Walk EQ → ExplorationThread → Exploration.id for recording the stored_result_use reference."
  [exploration-query]
  (t2/select-one-fn :exploration_id :model/ExplorationThread
                    :id (:exploration_thread_id exploration-query)))

(defn- compute-data-access-token
  "The creator's effective-data-access token for `dataset-query` — the sandbox/impersonation/routing
  fingerprint the snapshot is computed under, stored on the `StoredResult` and compared against a
  viewer's token to gate cached reads. Must be called inside the creator's `with-current-user` (+
  routing-on) binding. Best-effort: any failure yields nil, which the read gate treats as
  creator+admin-only."
  [dataset-query db-id]
  (try
    (perms/data-access-token {:database-id db-id
                              :table-ids   (query-perms/query->source-table-ids dataset-query)})
    (catch Throwable e
      (log/warn e "Failed to compute data-access token for exploration query result")
      nil)))

(defn- compute-query-result
  "The slow half of running `row`: the warehouse query, its serialization, and the chart-config /
  stats / score. Returns everything [[persist-query-result!]] needs to write.

  Deliberately holds no transaction.

  The query runs as the exploration's creator, so the snapshot reflects the creator's own lens —
  sandboxing, connection impersonation, and database routing (all applied by the QP's own
  middleware under the bound user). The same lens is captured as a `:data_access_token` so
  non-creator readers can be gated against it."
  [row]
  (let [creator-id (exploration-creator-id row)
        db-id      (:database_id row)
        run        (fn []
                     {:qp-result (qp.variants/pin-other-last
                                  (:query_type row)
                                  (qp/process-query
                                   (qp/userland-query-with-default-constraints
                                    (:dataset_query row)
                                    {:context :exploration})))
                      :token     (compute-data-access-token (:dataset_query row) db-id)})
        {:keys [qp-result token]} (if creator-id
                                    (request/with-current-user creator-id
                                      (run))
                                    (run))
        chart-config (safe-chart-config row qp-result)
        stats        (safe-deep-stats row chart-config)]
    {:creator-id creator-id
     :db-id      db-id
     :bytes      (serialize-result qp-result)
     :row-count  (:row_count qp-result)
     :token      token
     :stats      stats
     :score      (safe-score row chart-config stats)}))

(defn- persist-query-result!
  "Write what [[compute-query-result]] produced and flip the query to `done` transactionally.

  Returns false when a peer delivery already persisted this query."
  [row ^OffsetDateTime started {:keys [creator-id db-id bytes row-count token stats score]}]
  (try
    (t2/with-transaction [_conn]
      (let [sr-id (first
                   (t2/insert-returning-pks!
                    :model/StoredResult
                    {:result_data       bytes
                     :creator_id        creator-id
                     :database_id       db-id
                     :dataset_query     (:dataset_query row)
                     :row_count         row-count
                     :data_access_token token}))]
        (t2/insert! :model/ExplorationQueryResult
                    {:exploration_query_id  (:id row)
                     :stored_result_id      sr-id
                     :chart_stats           stats
                     :interestingness_score score})
        ;; Record the (exploration -> stored_result) reference for lifecycle/GC tracking.
        (t2/insert! :model/StoredResultUse
                    {:stored_result_id sr-id
                     :exploration_id   (exploration-id row)})
        (t2/update! :model/ExplorationQuery (:id row)
                    {:status      "done"
                     :started_at  started
                     :finished_at (OffsetDateTime/now)})))
    true
    (catch Exception e
      (if (t2/exists? :model/ExplorationQueryResult :exploration_query_id (:id row))
        (do (log/infof "ExplorationQuery %d was already completed by a peer; discarding this run's duplicate result"
                       (:id row))
            false)
        (throw e)))))

(defn run-query!
  "Execute the pending `ExplorationQuery` `query-id`, flip it to `done`, and return its thread id.

  Also returns the thread id — without re-running anything — for a query that is *already* `done`:
  the delivery that ran it may have persisted the result but then failed to run its follow-up
  completion check, and the redelivery is what re-runs that check (which is idempotent). Skipping it
  there could strand the thread short of completion. Returns nil when there is nothing to do: the
  query is still pending (or on a canceled thread), terminally `error`, or gone."
  [query-id]
  (if-let [row (runnable-query query-id)]
    (let [row      (finalize-row! row)
          started  (OffsetDateTime/now)
          computed (compute-query-result row)]
      (when (persist-query-result! row started computed)
        (record-query-outcome! "done"))
      (:exploration_thread_id row))
    (t2/select-one-fn :exploration_thread_id :model/ExplorationQuery
                      :id query-id :status "done")))

(defn fail-query!
  "Terminally mark `query-id` as `error` with `message`, the user-visible failure state the UI
  renders.

  No-ops on a row that is no longer `pending` (a later delivery succeeded, or it was canceled)."
  [query-id message]
  (let [thread-id (t2/select-one-fn :exploration_thread_id :model/ExplorationQuery :id query-id)]
    (when (pos? (t2/update! :model/ExplorationQuery
                            {:id query-id :status "pending"}
                            {:status        "error"
                             :error_message message
                             :finished_at   (OffsetDateTime/now)}))
      (record-query-outcome! "error"))
    thread-id))

(defn- canceled-mid-plan-cleanup!
  "Planner-race repair: the user can cancel a thread *after* the plan message was published but
  *before* the planner inserted its rows. The cancel endpoint's bulk pending→canceled UPDATE only
  saw the rows that existed at cancel time; rows the planner inserted after that are still `pending`
  on a canceled thread. Flip them so the query table matches its owning thread's terminal state."
  [thread-id]
  (when (t2/exists? :model/ExplorationThread :id thread-id :canceled_at [:not= nil])
    (t2/update! :model/ExplorationQuery
                {:exploration_thread_id thread-id
                 :status                "pending"}
                {:status "canceled"})))

(defn plan-thread!
  "Run the LLM planner for `thread-id`, materializing its `ExplorationQuery` rows. Idempotent for MQ."
  [thread-id]
  (let [thread   (t2/select-one [:model/ExplorationThread :id :canceled_at] :id thread-id)
        planned? (cond
                   ;; `restart` deletes and re-creates a thread's work; a message for a thread that
                   ;; no longer exists is a no-op.
                   (nil? thread)
                   false

                   ;; The user canceled between publishing the message and delivering it. Don't
                   ;; spend an LLM call on work nobody is waiting for.
                   (:canceled_at thread)
                   (do (log/infof "Exploration thread %d was canceled; skipping planning" thread-id)
                       false)

                   (t2/exists? :model/ExplorationQuery :exploration_thread_id thread-id)
                   (do (log/infof "Exploration thread %d is already planned; skipping" thread-id)
                       false)

                   :else
                   (do (explorations.query-plan/generate-query-plan! thread-id)
                       true))]
    (canceled-mid-plan-cleanup! thread-id)
    planned?))

(defn fail-plan!
  "Durably record that the queue gave up on planning `thread-id`: write the same terminal state
  the planner's own failure path does (transcript, planning-failed doc, terminal stamp), so the
  client stops polling and sees why instead of an exploration that silently never fills in.
  `message` is the error that exhausted the retries.

  A thread that already has query rows is left alone - planning succeeded there, and a failing
  duplicate delivery must not stamp 'planning failed' over work that is in flight. Also flips any
  rows a canceled thread left `pending`."
  [thread-id message]
  (explorations.query-plan/record-terminal-planning-failure! thread-id message)
  (canceled-mid-plan-cleanup! thread-id))

(defn pending-query-ids
  "Ids of `thread-id`'s queries still awaiting execution."
  [thread-id]
  (t2/select-pks-vec :model/ExplorationQuery :exploration_thread_id thread-id :status "pending"))
