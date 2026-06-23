(ns metabase.explorations.task.runner
  "Background worker for the Explorations pipeline. In this slice it drains two queues:

  *Planning* — it claims a thread that has been started but never planned
  (`started_at IS NOT NULL`, `query_plan_started_at IS NULL`, no `exploration_query`
  rows yet) via a CAS on `query_plan_started_at`, runs the planner, and materializes the
  resulting `:model/ExplorationQuery` rows (status `pending`).

  *Execution* — it claims one pending `:model/ExplorationQuery` row with `FOR UPDATE SKIP
  LOCKED`, runs the snapshotted MBQL through the QP, writes the serialized result to
  `:model/ExplorationQueryResult` (backed by a `:model/StoredResult`), and commits the whole
  thing in a single transaction. Crash recovery is automatic: a JVM kill drops the connection,
  the DB rolls back the tx, and the row is left as `pending` for another worker to pick up.

  Later slices extend the worker loop with the scoring, timeline, and completion phases."
  (:require
   [medley.core :as m]
   [metabase.app-db.core :as mdb]
   [metabase.explorations.query-plan :as explorations.query-plan]
   [metabase.explorations.query-plan.context :as qp.context]
   [metabase.explorations.query-plan.variants :as qp.variants]
   [metabase.explorations.settings :as explorations.settings]
   [metabase.lib.core :as lib]
   [metabase.permissions.core :as perms]
   [metabase.query-permissions.core :as query-perms]
   [metabase.query-processor.core :as qp]
   [metabase.query-processor.middleware.cache.impl :as cache.impl]
   [metabase.request.core :as request]
   [metabase.startup.core :as startup]
   [metabase.util.log :as log]
   [toucan2.core :as t2])
  (:import
   (java.time OffsetDateTime)))

(set! *warn-on-reflection* true)

(defn- worker-count
  "How many concurrent workers to spin up. H2 has no `FOR UPDATE SKIP LOCKED` so we'd race on
  the claim and double-insert into `exploration_query_result` (1:1 with `exploration_query`);
  cap at 1 there. Postgres/MySQL claim safely via SKIP LOCKED."
  []
  (case (mdb/db-type)
    :h2 1
    (explorations.settings/explorations-worker-count)))

(def ^:private idle-sleep-ms    1000)
(def ^:private error-backoff-ms 5000)
(def ^:private join-timeout-ms  5000)

(defonce ^:private running? (atom false))
(defonce ^:private threads  (atom []))

(defn- claim-pending-query
  "Claim a single pending row with `FOR UPDATE SKIP LOCKED` so peer workers don't fight for it.
  H2 doesn't support SKIP LOCKED, but we cap H2 at one worker (see `worker-count`) so dropping
  the lock clause is safe there."
  []
  (t2/select-one :model/ExplorationQuery
                 (cond-> {:select   [:*]
                          :from     [:exploration_query]
                          :where    [:= :status "pending"]
                          :order-by [[:id :asc]]
                          :limit    1}
                   (not= :h2 (mdb/db-type)) (assoc :for [:update :skip-locked]))))

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
  (cache.impl/do-with-serialization
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

(defn- execute-and-persist-query-result!
  "Run the QP on `row`'s `:dataset_query`, persist a `StoredResult` + `ExplorationQueryResult` +
  `StoredResultUse`, and flip the `ExplorationQuery` to `done`. `started` is the `OffsetDateTime`
  to stamp as the row's `:started_at`.

  The query runs as the exploration's creator, so the snapshot reflects the creator's own lens —
  sandboxing, connection impersonation, and database routing (all applied by the QP's own
  middleware under the bound user). The same lens is captured as a `:data_access_token` so
  non-creator readers can be gated against it."
  [row ^OffsetDateTime started]
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
        bytes        (serialize-result qp-result)
        sr-id        (first
                      (t2/insert-returning-pks!
                       :model/StoredResult
                       {:result_data       bytes
                        :creator_id        creator-id
                        :database_id       db-id
                        :dataset_query     (:dataset_query row)
                        :data_access_token token}))]
    (t2/insert! :model/ExplorationQueryResult
                {:exploration_query_id (:id row)
                 :stored_result_id     sr-id})
    ;; Record the (exploration -> stored_result) reference for lifecycle/GC tracking.
    (t2/insert! :model/StoredResultUse
                {:stored_result_id sr-id
                 :exploration_id   (exploration-id row)})
    (t2/update! :model/ExplorationQuery (:id row)
                {:status      "done"
                 :started_at  started
                 :finished_at (OffsetDateTime/now)})))

(defn- run-one-query-iteration!
  "Try to claim and execute a single pending query. Returns truthy when work was done so the
  caller knows whether to sleep."
  []
  (t2/with-transaction [_conn]
    (when-let [row (claim-pending-query)]
      (let [started (OffsetDateTime/now)]
        (try
          (execute-and-persist-query-result! (finalize-row! row) started)
          (catch Throwable e
            (log/errorf e "ExplorationQuery %d failed" (:id row))
            (t2/update! :model/ExplorationQuery (:id row)
                        {:status        "error"
                         :error_message (.getMessage e)
                         :started_at    started
                         :finished_at   (OffsetDateTime/now)}))))
      :worked)))

(defn- claim-unplanned-thread!
  "CAS-claim the next thread that has been started but never planned:
  `started_at IS NOT NULL`, `query_plan_started_at IS NULL`, and no
  `exploration_query` rows yet. Returns the claimed thread id, or nil when
  there's no work or we lost the race. The CAS makes this safe under multiple
  workers: only one update will affect a given row."
  []
  (let [thread-id (t2/select-one-fn
                   :id :model/ExplorationThread
                   {:where    [:and
                               [:not= :started_at nil]
                               [:= :query_plan_started_at nil]
                               [:not-exists {:select [1]
                                             :from   [:exploration_query]
                                             :where  [:= :exploration_query.exploration_thread_id
                                                      :exploration_thread.id]}]]
                    :order-by [[:id :asc]]
                    :limit    1})]
    (when thread-id
      (when (pos? (t2/update! :model/ExplorationThread
                              :id                    thread-id
                              :query_plan_started_at nil
                              {:query_plan_started_at (OffsetDateTime/now)}))
        thread-id))))

(defn- run-one-plan-iteration!
  "Try to claim one unplanned thread and run the planner against it. Returns
  truthy when work was done so the worker loop knows whether to sleep."
  []
  (when-let [thread-id (claim-unplanned-thread!)]
    (try
      (explorations.query-plan/generate-query-plan! thread-id)
      (catch Throwable e
        (log/errorf e "Exploration thread %d: query plan iteration crashed" thread-id)))
    :worked))

(defn- run-one-iteration!
  "Do one unit of work: plan any unplanned threads first, then execute pending
  queries. Returns truthy when something was processed."
  []
  (or (run-one-plan-iteration!)
      (run-one-query-iteration!)))

(defn- worker-loop
  [worker-id]
  (log/infof "Exploration worker %d started" worker-id)
  (while @running?
    (try
      (when-not (run-one-iteration!)
        (Thread/sleep ^long idle-sleep-ms))
      (catch InterruptedException _
        (reset! running? false))
      (catch Throwable e
        (log/errorf e "Exploration worker %d unexpected error" worker-id)
        (try (Thread/sleep ^long error-backoff-ms)
             (catch InterruptedException _ (reset! running? false))))))
  (log/infof "Exploration worker %d stopped" worker-id))

(defn- stop-workers!
  []
  (when (compare-and-set! running? true false)
    (doseq [^Thread t @threads]
      (.interrupt t))
    (doseq [^Thread t @threads]
      (.join t ^long join-timeout-ms))
    (reset! threads [])))

(defn- start-workers!
  []
  (when (compare-and-set! running? false true)
    (reset! threads
            (vec (for [i (range (worker-count))]
                   (doto (Thread. ^Runnable #(worker-loop i)
                                  (str "exploration-worker-" i))
                     (.setDaemon true)
                     (.start)))))
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable stop-workers! "exploration-worker-shutdown"))))

(defmethod startup/def-startup-logic! ::ExplorationRunner [_]
  (start-workers!))
