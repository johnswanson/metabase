(ns metabase.explorations.task.runner
  "Background worker for the Explorations pipeline. In this slice it drains the
  *planning* queue: it claims a thread that has been started but never planned
  (`started_at IS NOT NULL`, `query_plan_started_at IS NULL`, no `exploration_query`
  rows yet) via a CAS on `query_plan_started_at`, runs the mechanical planner, and
  materializes the resulting `:model/ExplorationQuery` rows (status `pending`).

  Later slices extend the worker loop with the execution, scoring, and timeline
  phases."
  (:require
   [metabase.app-db.core :as mdb]
   [metabase.explorations.query-plan :as explorations.query-plan]
   [metabase.explorations.settings :as explorations.settings]
   [metabase.startup.core :as startup]
   [metabase.util.log :as log]
   [toucan2.core :as t2])
  (:import
   (java.time OffsetDateTime)))

(set! *warn-on-reflection* true)

(defn- worker-count
  "How many concurrent workers to spin up. H2 has no `FOR UPDATE SKIP LOCKED` so we
  cap at 1 there; Postgres/MySQL claim safely via SKIP LOCKED."
  []
  (case (mdb/db-type)
    :h2 1
    (explorations.settings/explorations-worker-count)))

(def ^:private idle-sleep-ms    1000)
(def ^:private error-backoff-ms 5000)
(def ^:private join-timeout-ms  5000)

(defonce ^:private running? (atom false))
(defonce ^:private threads  (atom []))

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
  "Do one unit of work: plan any unplanned threads. Returns truthy when something
  was processed."
  []
  (run-one-plan-iteration!))

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
