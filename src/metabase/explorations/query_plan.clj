(ns metabase.explorations.query-plan
  "Orchestrator for the Explorations query planner.

  Selects a planner (the deterministic mechanical planner in this slice),
  dispatches through the `metabase.explorations.query-plan.planner/QueryPlanner`
  protocol, materializes the returned plan items into `ExplorationQuery` rows via
  the variant builders, and persists the full transcript to
  `exploration_thread.query_plan_transcript`."
  (:require
   [metabase.explorations.query-plan.context :as qp.context]
   [metabase.explorations.query-plan.mechanical :as qp.mechanical]
   [metabase.explorations.query-plan.planner :as planner]
   [metabase.explorations.query-plan.variants :as qp.variants]
   [metabase.explorations.settings :as explorations.settings]
   [metabase.util.date-2 :as u.date]
   [metabase.util.log :as log]
   [toucan2.core :as t2])
  (:import
   (java.time Instant)))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Planner selection
;; ---------------------------------------------------------------------------

(defn pick-planner!
  "Decide which planner to invoke. Returns a `QueryPlanner` instance — the
  deterministic mechanical planner. Public so tests can `with-redefs` it to
  inject a stub planner."
  []
  qp.mechanical/planner)

;; ---------------------------------------------------------------------------
;; Plan materialization (planner-agnostic)
;; ---------------------------------------------------------------------------

(defn- segment-for
  [metric segment-id]
  (when segment-id
    (some #(when (= segment-id (:id %)) %) (:segments metric))))

(defn- materialize-item
  "Translate one plan item into a vector of row *recipes* via the variant's
  `plan-rows` multimethod, then enrich each recipe with its localized `:name`."
  [metric-by-key item]
  (let [metric    (get metric-by-key [(:group_id item) (:metric_id item)])
        appl      (get-in metric [:applicability (:dimension_id item)])
        dim       (:dim appl)
        dim-label (or (:display_name dim) (:dimension_id dim))
        item-seg  (segment-for metric (get-in item [:params :segment_id]))
        plan-ctx  {:segment item-seg :params (:params item)}]
    (mapv (fn [recipe]
            (assoc recipe :name
                   (qp.variants/plan-time-name
                    (:query_type recipe)
                    {:card      (:card metric)
                     :dim-label dim-label
                     :segment   (segment-for metric (:segment_id recipe))
                     :params    (:params recipe)})))
          (qp.variants/plan-rows (:variant item) plan-ctx))))

(defn- insert-plan-rows!
  "Materialize each plan item into row recipes and insert them as
  `ExplorationQuery` rows. Returns the number of rows inserted."
  [thread-id metric-by-key plan]
  (let [rows (vec
              (for [item   plan
                    :let   [metric (get metric-by-key [(:group_id item) (:metric_id item)])]
                    recipe (try
                             (materialize-item metric-by-key item)
                             (catch Throwable e
                               (log/warnf e "Skipping plan item that failed to materialize: %s"
                                          (pr-str item))
                               []))]
                {:exploration_thread_id thread-id
                 :group_id              (:group_id item)
                 :card_id               (:metric_id item)
                 :database_id           (:database_id (:card metric))
                 :segment_id            (:segment_id recipe)
                 :dimension_id          (:dimension_id item)
                 :query_type            (:query_type recipe)
                 :display               (:display recipe)
                 :name                  (:name recipe)
                 :params                (:params recipe)
                 :status                "pending"}))]
    (when (seq rows)
      (t2/insert! :model/ExplorationQuery
                  (map-indexed (fn [i r] (assoc r :position i)) rows)))
    (count rows)))

;; ---------------------------------------------------------------------------
;; Transcript persistence
;; ---------------------------------------------------------------------------

(defn- save-transcript!
  [thread-id transcript]
  (try
    (t2/update! :model/ExplorationThread thread-id
                {:query_plan_transcript transcript})
    (catch Throwable e
      (log/warnf e "Failed to save query-plan transcript for thread %d" thread-id))))

(defn- record-outcome!
  "Persist a transcript with `:outcome` (and any extra kv pairs) merged onto `pre`."
  [thread-id pre outcome & {:as extras}]
  (save-transcript! thread-id (assoc (merge pre extras) :outcome outcome)))

(defn- preamble
  "Common transcript preamble: who chose what, when, with which planner."
  [thread-id planner-name]
  {:generated-at (u.date/format (Instant/now))
   :thread-id    thread-id
   :planner      planner-name
   :setting      (explorations.settings/explorations-query-planner)})

;; ---------------------------------------------------------------------------
;; Ctx building
;; ---------------------------------------------------------------------------

(defn- thread-prompt-for
  [thread-id]
  (t2/select-one-fn :prompt :model/ExplorationThread :id thread-id))

(defn- creator-id-for-thread
  [thread-id]
  (t2/select-one-fn :creator_id :model/Exploration
                    {:join  [:exploration_thread
                             [:= :exploration_thread.exploration_id :exploration.id]]
                     :where [:= :exploration_thread.id thread-id]}))

(defn- build-planner-ctx
  "Build the planner-contract ctx the chosen planner consumes."
  [thread-id]
  (let [thread-groups  (t2/select :model/ExplorationThreadGroup
                                  :exploration_thread_id thread-id
                                  {:order-by [[:position :asc] [:id :asc]]})
        metric-dim-ctx (qp.context/metric-and-dim-context thread-groups)
        ;; [group-id metric-id] -> metric-context, so materialization resolves a plan
        ;; item against the same group the planner emitted it under (a metric can live
        ;; in several groups).
        metric-by-key  (into {}
                             (for [g (:groups metric-dim-ctx)
                                   m (:metrics g)]
                               [[(:group-id g) (:metric-id m)] m]))]
    {:thread-id      thread-id
     :thread-prompt  (thread-prompt-for thread-id)
     :metric-dim-ctx metric-dim-ctx
     :metric-by-key  metric-by-key
     :creator-id     (creator-id-for-thread thread-id)
     :thread-groups  thread-groups}))

;; ---------------------------------------------------------------------------
;; Public entry point — called from the worker
;; ---------------------------------------------------------------------------

(defn- run-planner!
  "Invoke the picked planner, persist rows as appropriate, and return the outcome
  keyword (`:ok`, `:skip-empty`, or `:failed`)."
  [{:keys [thread-id metric-by-key] :as ctx} picked planner-id pre]
  (let [{:keys [outcome plan rationale transcript final-errors]} (planner/plan! picked ctx)
        transcript-body {:outcome      outcome
                         :rationale    rationale
                         :plan         plan
                         :final-errors final-errors
                         :planner      transcript}]
    (case outcome
      :ok
      (let [n (insert-plan-rows! thread-id metric-by-key plan)]
        (record-outcome! thread-id pre :ok :rows-count n :transcript transcript-body)
        (log/infof "Query plan for thread %d (%s): inserted %d ExplorationQuery rows"
                   thread-id (name planner-id) n)
        :ok)

      :skip-not-applicable
      (do (log/infof "Query plan for thread %d (%s): planner reported nothing to do"
                     thread-id (name planner-id))
          (record-outcome! thread-id pre :skip-empty :transcript transcript-body)
          :skip-empty)

      :failed
      (do (log/warnf "Query plan for thread %d (%s): planner failed"
                     thread-id (name planner-id))
          (record-outcome! thread-id pre :failed :transcript transcript-body)
          :failed))))

(defn generate-query-plan!
  "Build a query plan for `thread-id` and materialize ExplorationQuery rows.

  Returns one of `:ok`, `:skip-empty` (thread has no group with both a metric and
  a dimension), `:failed` (planner reported failure), or `nil` (uncaught
  throwable; logged, transcript best-effort)."
  [thread-id]
  (try
    (let [{:keys [thread-groups] :as ctx} (build-planner-ctx thread-id)
          picked     (pick-planner!)
          planner-id (planner/planner-name picked)
          pre        (preamble thread-id planner-id)]
      (if (not-any? #(and (seq (:metrics %)) (seq (:dimensions %))) thread-groups)
        (do (log/infof "Thread %d: no group has both a metric and a dimension; skipping query plan" thread-id)
            (record-outcome! thread-id pre :skip-empty)
            :skip-empty)
        (run-planner! ctx picked planner-id pre)))
    (catch Throwable e
      (log/errorf e "generate-query-plan! failed for thread %d" thread-id)
      (record-outcome! thread-id (preamble thread-id :unknown) :error
                       :error (.getMessage e))
      nil)))

(defn debug-transcript
  "Return the persisted query-plan transcript for `thread-id`."
  [thread-id]
  (t2/select-one-fn :query_plan_transcript :model/ExplorationThread :id thread-id))
