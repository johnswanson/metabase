(ns metabase.explorations.task.runner-test
  (:require
   [clojure.test :refer :all]
   [metabase.explorations.interestingness :as explorations.interestingness]
   [metabase.explorations.models.exploration-query-result :as eqr]
   [metabase.explorations.task.runner :as runner]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.test :as mt]
   [toucan2.core :as t2])
  (:import
   (java.time OffsetDateTime)))

(set! *warn-on-reflection* true)

(def ^:private run-one-iteration! #'runner/run-one-iteration!)
(def ^:private claim-pending-query #'runner/claim-pending-query)
(def ^:private claim-unplanned-thread! #'runner/claim-unplanned-thread!)
(def ^:private canceled-mid-plan-cleanup! #'runner/canceled-mid-plan-cleanup!)

(defn- temp-thread!
  ([user-id] (temp-thread! user-id nil))
  ([user-id prompt]
   (let [exploration (first (t2/insert-returning-instances! :model/Exploration
                                                            {:name "runner-test"
                                                             :creator_id user-id}))]
     (first (t2/insert-returning-instances! :model/ExplorationThread
                                            (cond-> {:exploration_id (:id exploration)
                                                     :position       0}
                                              prompt (assoc :prompt prompt)))))))

(defn- thread-group!
  "Create a minimal ExplorationThreadGroup for `thread-id` and return its id. Query rows
  require a non-null group_id (FK to this table)."
  [thread-id]
  (t2/insert-returning-pk! :model/ExplorationThreadGroup
                           {:exploration_thread_id thread-id}))

(defn- pending-query!
  [thread-id card-id mbql]
  (first (t2/insert-returning-instances! :model/ExplorationQuery
                                         {:exploration_thread_id thread-id
                                          :card_id               card-id
                                          :database_id           (mt/id)
                                          :group_id              (thread-group! thread-id)
                                          :dimension_id          "d1"
                                          :dataset_query         mbql
                                          :status                "pending"
                                          :position              0})))

(defn- drain-until-terminal!
  "Repeatedly call `run-one-iteration!` until the row with `row-id` reaches a terminal state, or
  `max-iters` is exhausted. Necessary because other concurrent tests may have their own pending
  rows that get processed first."
  [row-id max-iters]
  (loop [n max-iters]
    (when (zero? n)
      (throw (ex-info "ran out of iterations waiting for row" {:row-id row-id})))
    (run-one-iteration!)
    (let [r (t2/select-one :model/ExplorationQuery :id row-id)]
      (if (#{"done" "error"} (:status r))
        r
        (recur (dec n))))))

(defn- stored-result-for
  "Fetch the stored_result row linked from the EQR for `eq-id`. Returns nil when nothing
  exists yet."
  [eq-id]
  (eqr/stored-results eq-id))

(defn- cancel-thread!
  "Stamp `canceled_at` + `completed_at` directly on the thread, the way the cancel endpoint does.
  Bypasses the API to keep these tests focused on the runner-side guards."
  [thread-id]
  (let [now (OffsetDateTime/now)]
    (t2/update! :model/ExplorationThread thread-id
                {:canceled_at now :completed_at now})))

(deftest run-one-iteration-happy-path-test
  (testing "A pending row gets executed, the linked stored_result holds result_data, and status flips to done"
    (mt/with-temp [:model/User u {:email "happy@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread (temp-thread! (:id u))
            row    (pending-query! (:id thread) (:id card)
                                   (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count))))))
            final  (drain-until-terminal! (:id row) 10)
            result (t2/select-one :model/ExplorationQueryResult
                                  :exploration_query_id (:id row))
            sr     (stored-result-for (:id row))]
        (is (= "done" (:status final)))
        (is (some? (:started_at final)))
        (is (some? (:finished_at final)))
        (is (nil? (:error_message final)))
        (is (some? result))
        (is (some? (:stored_result_id result)))
        (is (some? sr))
        (is (pos? (count (:result_data sr))))))))

(deftest run-one-iteration-records-stored-result-use-test
  (testing "Running a query records a stored_result_use row tying the snapshot to the exploration"
    (mt/with-temp [:model/User u {:email "sruse@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread  (temp-thread! (:id u))
            expl-id (t2/select-one-fn :exploration_id :model/ExplorationThread :id (:id thread))
            row     (pending-query! (:id thread) (:id card)
                                    (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count))))))
            _       (drain-until-terminal! (:id row) 10)
            sr-id   (:stored_result_id (t2/select-one :model/ExplorationQueryResult
                                                      :exploration_query_id (:id row)))
            use-row (t2/select-one :model/StoredResultUse :stored_result_id sr-id)]
        (is (some? use-row))
        (is (= expl-id (:exploration_id use-row)))
        (is (nil? (:card_id use-row)))))))

(deftest run-one-iteration-writes-interestingness-score-test
  (testing "A 2-column result gets scored and the score lands on the result row"
    (mt/with-temp [:model/User u {:email "score@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread (temp-thread! (:id u))
            row    (pending-query! (:id thread) (:id card)
                                   (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)) (lib/breakout (lib.metadata/field mp (mt/id :venues :category_id)))))))
            _      (drain-until-terminal! (:id row) 10)
            result (t2/select-one :model/ExplorationQueryResult
                                  :exploration_query_id (:id row))
            score  (:interestingness_score result)]
        (is (some? result))
        (is (double? score))
        (is (<= 0.0 score 1.0))))))

(deftest run-one-iteration-survives-scoring-failure-test
  (testing "A scoring exception leaves the row done with a nil score; the result blob is still written"
    (mt/with-temp [:model/User u {:email "scorefail@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread (temp-thread! (:id u))
            row    (pending-query! (:id thread) (:id card)
                                   (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)) (lib/breakout (lib.metadata/field mp (mt/id :venues :category_id)))))))]
        (mt/with-dynamic-fn-redefs [explorations.interestingness/qp-result->chart-config
                                    (fn [& _] (throw (ex-info "boom" {})))]
          (let [final  (drain-until-terminal! (:id row) 10)
                result (t2/select-one :model/ExplorationQueryResult
                                      :exploration_query_id (:id row))
                sr     (stored-result-for (:id row))]
            (is (= "done" (:status final)))
            (is (some? result))
            (is (some? sr))
            (is (pos? (count (:result_data sr))))
            (is (nil? (:interestingness_score result)))))))))

(deftest run-one-iteration-error-path-test
  (testing "A row whose query blows up is marked error, no result row is written"
    (mt/with-temp [:model/User u {:email "err@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread (temp-thread! (:id u))
            row    (pending-query! (:id thread) (:id card)
                                   {:database 999999 :type :query
                                    :query {:source-table 1 :aggregation [[:count]]}})
            final  (drain-until-terminal! (:id row) 10)]
        (is (= "error" (:status final)))
        (is (some? (:error_message final)))
        (is (some? (:finished_at final)))
        (is (zero? (t2/count :model/ExplorationQueryResult
                             :exploration_query_id (:id row))))))))

(deftest claim-pending-query-skips-canceled-thread-test
  (testing "A pending EQ whose owning thread has canceled_at set is invisible to the claim query"
    (mt/with-temp [:model/User u {:email "cancel-claim@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread (temp-thread! (:id u))
            row    (pending-query! (:id thread) (:id card)
                                   (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count))))))]
        (cancel-thread! (:id thread))
        (let [claimed (claim-pending-query)]
          (is (or (nil? claimed) (not= (:id row) (:id claimed)))
              "claim-pending-query must not return a row on a canceled thread"))))))

(deftest claim-unplanned-thread-skips-canceled-test
  (testing "An unplanned, started, canceled thread is not claimed by the planner"
    (mt/with-temp [:model/User u {:email "cancel-plan@example.com"}]
      (let [thread (temp-thread! (:id u))]
        (t2/update! :model/ExplorationThread (:id thread) {:started_at (OffsetDateTime/now)})
        (cancel-thread! (:id thread))
        ;; Other tests may have unplanned threads in flight; just assert ours isn't picked.
        (let [claimed (claim-unplanned-thread!)]
          (is (not= (:id thread) claimed)
              "canceled threads must not be claimed for planning"))))))

(deftest canceled-mid-plan-cleanup-flips-pending-rows-test
  (testing "Planner-race repair: when the planner finishes for a thread the user canceled mid-plan,
            the cleanup helper flips the just-inserted pending rows to canceled"
    (mt/with-temp [:model/User u {:email "cancel-cleanup@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread     (temp-thread! (:id u))
            pending-eq (pending-query! (:id thread) (:id card)
                                       (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count))))))]
        (cancel-thread! (:id thread))
        (canceled-mid-plan-cleanup! (:id thread))
        (is (= "canceled" (:status (t2/select-one :model/ExplorationQuery :id (:id pending-eq))))
            "pending EQ on a canceled thread must be flipped to canceled")))))

(deftest canceled-mid-plan-cleanup-noop-on-uncanceled-test
  (testing "cleanup helper does nothing for threads that aren't canceled — common-case no-op"
    (mt/with-temp [:model/User u {:email "cancel-cleanup-noop@example.com"}
                   :model/Card card {:type :metric
                                     :creator_id (:id u)
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
      (let [thread     (temp-thread! (:id u))
            pending-eq (pending-query! (:id thread) (:id card)
                                       (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count))))))]
        (canceled-mid-plan-cleanup! (:id thread))
        (is (= "pending" (:status (t2/select-one :model/ExplorationQuery :id (:id pending-eq))))
            "pending EQ on a live thread must be left alone")))))
