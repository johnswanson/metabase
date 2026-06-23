(ns metabase.explorations.task.runner-test
  (:require
   [clojure.test :refer :all]
   [metabase.explorations.models.exploration-query-result :as eqr]
   [metabase.explorations.task.runner :as runner]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(def ^:private run-one-iteration! #'runner/run-one-iteration!)

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
