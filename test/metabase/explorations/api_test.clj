(ns metabase.explorations.api-test
  (:require
   [clojure.test :refer :all]
   [java-time.api :as t]
   [metabase.collections.models.collection :as collection]
   [metabase.config.core :as config]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.permissions.core :as perms]
   [metabase.permissions.models.permissions-group :as perms-group]
   [metabase.queries.models.card :as card]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]
   [toucan2.core :as t2]))

(use-fixtures :once (fixtures/initialize :db :web-server :test-users))

(defn- do-with-sample-metrics-archived
  "Temporarily archive any metric cards belonging to the sample database so they
   don't interfere with test assertions. Restores them after `thunk` completes."
  [thunk]
  (let [sample-db-id   (t2/select-one-pk :model/Database :is_sample true)
        metric-ids     (when sample-db-id
                         (t2/select-pks-vec :model/Card
                                            :type :metric
                                            :archived false
                                            :database_id sample-db-id))]
    (if (seq metric-ids)
      (try
        (t2/query {:update :report_card
                   :set    {:archived true}
                   :where  [:in :id metric-ids]})
        (thunk)
        (finally
          (t2/query {:update :report_card
                     :set    {:archived false}
                     :where  [:in :id metric-ids]})))
      (thunk))))

(defmacro with-sample-metrics-archived
  "Execute `body` with any sample-database metric cards temporarily archived."
  [& body]
  `(do-with-sample-metrics-archived (fn [] ~@body)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                    GET /api/exploration/dimensions                                             |
;;; +----------------------------------------------------------------------------------------------------------------+

(deftest dimensions-returns-hydrated-metrics-test
  (testing "GET /api/exploration/dimensions returns metrics referencing dimensions by id"
    (with-sample-metrics-archived
      (mt/with-temp [:model/Card _m1 {:name          "Alpha Metric"
                                      :type          :metric
                                      :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}
                     :model/Card _m2 {:name          "Beta Metric"
                                      :type          :metric
                                      :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
        (let [response       (mt/user-http-request :rasta :get 200 "exploration/dimensions")
              all-metrics    (:metrics response)
              ;; Scope to this test's own metrics — other tests' temp :metric Cards can be live
              ;; in the catalog during a parallel run, so an exact total count isn't reliable.
              metrics        (filter #(#{"Alpha Metric" "Beta Metric"} (:name %)) all-metrics)
              groups         (:dimension_groups response)
              metric-dim-ids (set (mapcat :dimension_ids all-metrics))]
          (is (= 2 (count metrics)) "both of this test's metrics are present")
          (is (every? #(contains? % :dimension_ids) metrics))
          (is (every? #(not (contains? % :dimensions)) metrics))
          (is (every? #(contains? % :dimension_mappings) metrics))
          (testing "every group has a name and a non-empty dimension list"
            (is (every? :name groups))
            (is (every? #(seq (:dimensions %)) groups)))
          (testing "every grouped dimension belongs to some metric's dimension_ids"
            ;; groups drop dimensions scoring below min-interestingness, so the reverse
            ;; (every metric dimension appears in a group) no longer holds.
            (doseq [g groups]
              (is (every? #(contains? metric-dim-ids (:id %)) (:dimensions g))))))))))

(deftest dimensions-search-by-name-test
  (testing "GET /api/exploration/dimensions filters case-insensitively by metric name"
    (with-sample-metrics-archived
      (mt/with-temp [:model/Card _m1 {:name          "Revenue"
                                      :type          :metric
                                      :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}
                     :model/Card _m2 {:name          "Order Count"
                                      :type          :metric
                                      :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
        (let [response (mt/user-http-request :rasta :get 200 "exploration/dimensions" :q "reven")]
          (is (= 1 (count (:metrics response))))
          (is (= "Revenue" (:name (first (:metrics response))))))))))

(deftest dimensions-search-by-dimension-display-name-test
  (testing "GET /api/exploration/dimensions matches metrics whose dimension display-name contains q"
    (with-sample-metrics-archived
      (mt/with-temp [:model/Card metric {:name          "Sales"
                                         :type          :metric
                                         :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
        (let [hydrated (mt/user-http-request :rasta :get 200 (str "metric/" (:id metric)))
              dim-name (some-> hydrated :dimensions first :display_name)]
          (is (some? dim-name) "metric should have at least one hydrated dimension with a display_name")
          (let [response (mt/user-http-request :rasta :get 200 "exploration/dimensions"
                                               :q (subs dim-name 0 (min 3 (count dim-name))))]
            (is (some #(= (:id metric) (:id %)) (:metrics response))
                "metric should be returned because a dimension display-name matched")))))))

(deftest dimensions-search-no-match-test
  (testing "GET /api/exploration/dimensions returns empty lists when nothing matches"
    (with-sample-metrics-archived
      (mt/with-temp [:model/Card _m {:name          "Hello"
                                     :type          :metric
                                     :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
        (let [response (mt/user-http-request :rasta :get 200 "exploration/dimensions"
                                             :q "zzz_no_such_thing_zzz")]
          (is (= [] (:metrics response)))
          (is (= [] (:dimension_groups response))))))))

(deftest dimensions-respects-collection-perms-test
  (testing "GET /api/exploration/dimensions excludes metrics in collections the user can't read"
    (with-sample-metrics-archived
      (mt/with-non-admin-groups-no-root-collection-perms
        (mt/with-temp [:model/Collection collection {}
                       :model/Card _hidden {:name          "Hidden Metric"
                                            :type          :metric
                                            :collection_id (:id collection)
                                            :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
          (let [response (mt/user-http-request :rasta :get 200 "exploration/dimensions")]
            (is (not-any? #(= "Hidden Metric" (:name %)) (:metrics response)))))))))

(deftest dimensions-drops-unresolvable-dimensions-test
  (testing "GET /api/exploration/dimensions silently drops dimensions whose target ref doesn't resolve against the metric's dataset_query (UXW-4083)"
    (with-sample-metrics-archived
      (mt/with-temp [:model/Card metric {:name          "Filter target"
                                         :type          :metric
                                         :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}]
        (let [good-id "11111111-1111-1111-1111-111111111111"
              bad-id  "22222222-2222-2222-2222-222222222222"
              good-mapping {:dimension_id good-id
                            :type         :table
                            :target       [:field {} (mt/id :venues :name)]
                            :table_id     (mt/id :venues)}
              bad-mapping  {:dimension_id bad-id
                            :type         :table
                            :target       [:field {} 999999999]
                            :table_id     (mt/id :venues)}]
          ;; Replace whatever sync wrote with a minimal pair: one resolvable, one not.
          ;; Suppress the after-update auto-sync so it can't reconcile our hand-crafted
          ;; row back to the computed dimensions.
          (with-redefs [card/*syncing-metric-dimensions* true]
            (t2/update! :model/Card (:id metric)
                        {:dimensions         [{:id good-id :name "NAME" :display_name "Good"
                                               :effective_type :type/Text}
                                              {:id bad-id  :name "ZZZ"  :display_name "Unresolvable"
                                               :effective_type :type/Text}]
                         :dimension_mappings [good-mapping bad-mapping]}))
          (let [response   (mt/user-http-request :rasta :get 200 "exploration/dimensions")
                the-metric (first (filter #(= (:id metric) (:id %)) (:metrics response)))]
            (is (some? the-metric) "metric should be present in response")
            (is (contains? (set (:dimension_ids the-metric)) good-id)
                "resolvable dimension should be retained")
            (is (not (contains? (set (:dimension_ids the-metric)) bad-id))
                "dimension whose target field doesn't exist should be silently dropped")
            (is (= [good-id] (mapv :dimension_id (:dimension_mappings the-metric)))
                "matching mapping should be dropped too")))))))

(deftest exploration-create-without-selections-test
  (testing "POST / works without metrics/dimensions/timelines (drafty exploration)"
    (mt/with-temp [:model/User u {:email "empty@example.com"}]
      (let [resp (mt/user-http-request u :post 200 "exploration" {:name "empty"})]
        (is (= 1 (count (:threads resp))))
        (is (zero? (count (-> resp :threads first :metrics))))
        (is (zero? (count (-> resp :threads first :queries))))))))

(deftest exploration-get-permissions-test
  (testing "Only the creator (or a superuser) can GET an exploration"
    (mt/with-temp [:model/User owner {:email "p-owner@example.com"}
                   :model/User other {:email "p-other@example.com"}]
      (let [{eid :id} (mt/user-http-request owner :post 200 "exploration"
                                            {:name "private"
                                             :collection_id (:id (collection/user->personal-collection (:id owner)))})]
        (mt/user-http-request other :get 403 (format "exploration/%d" eid))
        (let [resp (mt/user-http-request owner :get 200 (format "exploration/%d" eid))]
          (is (= eid (:id resp))))))))

(deftest exploration-cascade-delete-test
  (testing "Deleting an exploration cascades to threads"
    (mt/with-temp [:model/User u {:email "cd@example.com"}]
      (let [resp (mt/user-http-request u :post 200 "exploration" {:name "cascade"})
            eid  (:id resp)
            tid  (-> resp :threads first :id)]
        (t2/delete! :model/Exploration :id eid)
        (is (false? (t2/exists? :model/ExplorationThread :id tid)))
        (is (zero? (t2/count :model/ExplorationThread :exploration_id eid)))))))

(deftest exploration-http-delete-returns-204-test
  (testing "DELETE /api/exploration/:id returns 204 and removes the row — guards a malli regression where returning the Ring response map `generic-204-no-content` instead of literal `nil` made the `:- :nil` schema reject the response and yield a 400"
    (mt/with-temp [:model/User u {:email "http-delete@example.com"}]
      (let [resp (mt/user-http-request u :post 200 "exploration" {:name "http-delete"})
            eid  (:id resp)]
        ;; Live exploration: delete via HTTP.
        (mt/user-http-request u :delete 204 (format "exploration/%d" eid))
        (is (false? (t2/exists? :model/Exploration :id eid))))
      (testing "archived (trashed) exploration deletes via HTTP DELETE with the same status — the trash → permanently-delete path the user reported as 400"
        (let [resp2 (mt/user-http-request u :post 200 "exploration" {:name "trash-then-delete"})
              eid2  (:id resp2)]
          (mt/user-http-request u :put 200 (format "exploration/%d" eid2) {:archived true})
          (mt/user-http-request u :delete 204 (format "exploration/%d" eid2))
          (is (false? (t2/exists? :model/Exploration :id eid2))))))))

(deftest exploration-put-updates-metadata-test
  (testing "PUT /:id updates name/description for the creator"
    (mt/with-temp [:model/Exploration e {:name "old" :creator_id (mt/user->id :rasta)}]
      (let [resp (mt/user-http-request :rasta :put 200 (format "exploration/%d" (:id e))
                                       {:name "new" :description "yo"})]
        (is (= "new" (:name resp)))
        (is (= "yo"  (:description resp)))))))

(deftest exploration-create-in-collection-test
  (testing "POST / places the exploration in the requested collection when the caller can write it"
    (mt/with-temp [:model/Collection c {}]
      (mt/with-non-admin-groups-no-collection-perms (:id c)
        (perms/grant-collection-readwrite-permissions! (perms-group/all-users) c)
        (let [resp (mt/user-http-request :rasta :post 200 "exploration"
                                         {:name "in-coll" :collection_id (:id c)})]
          (is (= (:id c) (:collection_id resp))))))))

(deftest exploration-create-defaults-to-root-test
  (testing "POST / with no :collection_id leaves the exploration in the root collection"
    (let [resp (mt/user-http-request :rasta :post 200 "exploration" {:name "rootish"})]
      (is (nil? (:collection_id resp))))))

(deftest exploration-create-requires-write-on-collection-test
  (testing "POST / refuses when the caller lacks write on the destination collection"
    (mt/with-temp [:model/Collection c {}]
      (mt/with-non-admin-groups-no-collection-perms (:id c)
        (mt/user-http-request :rasta :post 403 "exploration"
                              {:name "denied" :collection_id (:id c)})))))

(deftest exploration-put-move-to-collection-test
  (testing "PUT /:id can move an exploration into a collection the caller can write"
    (mt/with-temp [:model/Collection c {}
                   :model/Exploration e {:name "to-move" :creator_id (mt/user->id :rasta)}]
      (mt/with-non-admin-groups-no-collection-perms (:id c)
        (perms/grant-collection-readwrite-permissions! (perms-group/all-users) c)
        (let [resp (mt/user-http-request :rasta :put 200 (format "exploration/%d" (:id e))
                                         {:collection_id (:id c)})]
          (is (= (:id c) (:collection_id resp))))))))

(deftest exploration-put-move-requires-write-on-destination-test
  (testing "PUT /:id move refuses when caller lacks write on the destination collection"
    (mt/with-temp [:model/Collection c {}
                   :model/Exploration e {:name "no-dest" :creator_id (mt/user->id :rasta)}]
      (mt/with-non-admin-groups-no-collection-perms (:id c)
        (mt/user-http-request :rasta :put 403 (format "exploration/%d" (:id e))
                              {:collection_id (:id c)})))))

(deftest exploration-put-move-requires-write-on-source-collection-test
  (testing "Moving an exploration in a shared collection requires write on the source collection."
    (mt/with-temp [:model/Collection src  {}
                   :model/Collection dest {}
                   :model/Exploration e   {:name          "needs-src"
                                           :creator_id    (mt/user->id :rasta)
                                           :collection_id (:id src)}]
      (mt/with-non-admin-groups-no-collection-perms (:id src)
        (mt/with-non-admin-groups-no-collection-perms (:id dest)
          (perms/grant-collection-readwrite-permissions! (perms-group/all-users) dest)
          ;; user has dest write but no src perms — write-check on the exploration
          ;; (which goes through src collection perms) fails first.
          (mt/user-http-request :rasta :put 403 (format "exploration/%d" (:id e))
                                {:collection_id (:id dest)}))))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         Routed-database metrics                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(deftest dimensions-includes-routed-database-metrics-test
  (when config/ee-available?
    (testing "GET /api/exploration/dimensions includes metrics whose database is a router — routed
              results are gated per-lens on read (see stored_result.data_access_token) rather than
              hidden from the picker"
      (with-sample-metrics-archived
        (mt/with-temp [:model/Card        _ {:name          "Routed Metric"
                                             :type          :metric
                                             :dataset_query (lib/->legacy-MBQL (let [mp (mt/metadata-provider)] (-> (lib/query mp (lib.metadata/table mp (mt/id :venues))) (lib/aggregate (lib/count)))))}
                       :model/DatabaseRouter _ {:database_id    (mt/id)
                                                :user_attribute "team"}]
          (let [resp  (mt/user-http-request :rasta :get 200 "exploration/dimensions")
                names (set (map :name (:metrics resp)))]
            (is (contains? names "Routed Metric")
                "metric on a router database is selectable in /dimensions")))))))

(defn- touch-revision!
  "Insert a bare `revision` row attributing a touch of `model`/`id` to `user-id` at `ts`. The
  `/mine` query keys off `user_id` + `timestamp` only, so the snapshot can be empty."
  [model id user-id ts]
  (t2/insert! :model/Revision
              {:model model :model_id id :user_id user-id :object {}
               :timestamp ts :is_creation false :is_reversion false :most_recent false}))

(defn- m-index-by
  "Index a `GET /mine` response's `:data` rows by `:name`."
  [resp]
  (into {} (map (juxt :name identity)) (:data resp)))

(deftest mine-membership-and-permissions-test
  (testing "GET /mine returns explorations the caller created or edited, excluding moved-away and archived"
    (mt/with-temp [:model/User       me  {:email "mine-member@example.com"}
                   :model/User       other {:email "mine-other@example.com"}
                   :model/Collection readable {:name "readable"}
                   :model/Collection hidden   {:name "hidden"}]
      ;; Temp collections auto-grant All Users read-write; revoke it on `hidden` so the caller
      ;; (a fresh user, member of All Users only) genuinely cannot see it.
      (perms/revoke-collection-permissions! (perms-group/all-users) (:id hidden))
      (mt/with-temp [:model/Exploration _created  {:name "created-by-me" :creator_id (:id me) :collection_id (:id readable)}
                     :model/Exploration edited    {:name "edited-by-me"  :creator_id (:id other) :collection_id (:id readable)}
                     :model/Exploration _untouched {:name "untouched"    :creator_id (:id other) :collection_id (:id readable)}
                     :model/Exploration _moved    {:name "moved-away"    :creator_id (:id me) :collection_id (:id hidden)}
                     :model/Exploration _archived {:name "archived"      :creator_id (:id me) :collection_id (:id readable) :archived true}]
        ;; `me` edited an exploration `other` created.
        (touch-revision! "Exploration" (:id edited) (:id me) (t/offset-date-time))
        (let [resp (mt/user-http-request me :get 200 "exploration/mine")
              by-name (m-index-by resp)]
          (testing "created-by-me is present"
            (is (contains? by-name "created-by-me")))
          (testing "edited-by-me is present even though someone else created it"
            (is (contains? by-name "edited-by-me"))
            (testing "and the hydrated creator is the other user, not the caller"
              (is (= (:email other) (-> by-name (get "edited-by-me") :creator :email)))))
          (testing "an exploration the caller never touched is absent"
            (is (not (contains? by-name "untouched"))))
          (testing "an exploration moved into a collection the caller can't read is absent"
            (is (not (contains? by-name "moved-away"))))
          (testing "an archived exploration is absent"
            (is (not (contains? by-name "archived"))))
          (testing "total reflects the visible, post-filter count"
            (is (= 2 (:total resp))))
          (testing "rows don't leak the internal total_count column"
            (is (not (contains? (get by-name "created-by-me") :total_count)))))))))

(deftest mine-pagination-test
  (testing "GET /mine pages with a stable order and a post-filter total"
    (mt/with-temp [:model/User       me {:email "mine-page@example.com"}
                   :model/Collection coll {:name "page-coll"}
                   :model/Exploration _e1 {:name "e1" :creator_id (:id me) :collection_id (:id coll)}
                   :model/Exploration _e2 {:name "e2" :creator_id (:id me) :collection_id (:id coll)}
                   :model/Exploration _e3 {:name "e3" :creator_id (:id me) :collection_id (:id coll)}]
      (let [page1 (mt/user-http-request me :get 200 "exploration/mine" :limit 2 :offset 0)
            page2 (mt/user-http-request me :get 200 "exploration/mine" :limit 2 :offset 2)]
        (testing "total is the same across pages and reflects all three"
          (is (= 3 (:total page1)))
          (is (= 3 (:total page2))))
        (testing "the envelope echoes limit/offset"
          (is (= 2 (:limit page1)))
          (is (= 0 (:offset page1)))
          (is (= 2 (:offset page2))))
        (testing "the two pages partition the result set with no overlap"
          (is (= 2 (count (:data page1))))
          (is (= 1 (count (:data page2))))
          (is (= #{"e1" "e2" "e3"}
                 (into #{} (map :name) (concat (:data page1) (:data page2)))))))
      (testing "an unpaged request returns everything with nil limit/offset"
        (let [resp (mt/user-http-request me :get 200 "exploration/mine")]
          (is (nil? (:limit resp)))
          (is (nil? (:offset resp)))
          (is (= 3 (count (:data resp)))))))))
