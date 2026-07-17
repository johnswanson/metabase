(ns metabase.explorations.runner
  "The work an exploration does in the background, as two plain functions:

    [[plan-thread!]]  — ask the LLM which charts to build, and materialize them as
                        `:model/ExplorationQuery` rows
    [[run-query!]]    — run one query's MBQL through the QP and store the result

  Each is idempotent for calling from MQ."
  (:require
   [metabase.analytics-interface.core :as analytics.interface]
   [metabase.analytics.core :as analytics]
   [metabase.explorations.query-plan :as explorations.query-plan]
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

(defn plan-thread!
  "Run the LLM planner for `thread-id`, materializing its `ExplorationQuery` rows. Idempotent for MQ."
  [thread-id]
  (let [thread (t2/select-one [:model/ExplorationThread :id] :id thread-id)]
    (cond
      ;; `restart` deletes and re-creates a thread's work; a message for a thread that
      ;; no longer exists is a no-op.
      (nil? thread)
      false

      (t2/exists? :model/ExplorationQuery :exploration_thread_id thread-id)
      (do (log/infof "Exploration thread %d is already planned; skipping" thread-id)
          false)

      :else
      (do (explorations.query-plan/generate-query-plan! thread-id)
          true))))

(defn fail-plan!
  "Durably record that the queue gave up on planning `thread-id`: write the same terminal state
  the planner's own failure path does (transcript, planning-failed doc, terminal stamp), so the
  client stops polling and sees why instead of an exploration that silently never fills in.
  `message` is the error that exhausted the retries.

  A thread that already has query rows is left alone - planning succeeded there, and a failing
  duplicate delivery must not stamp 'planning failed' over work that is in flight."
  [thread-id message]
  (explorations.query-plan/record-terminal-planning-failure! thread-id message))

(defn pending-query-ids
  "Ids of `thread-id`'s queries still awaiting execution."
  [thread-id]
  (t2/select-pks-vec :model/ExplorationQuery :exploration_thread_id thread-id :status "pending"))
