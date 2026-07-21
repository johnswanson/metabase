(ns metabase.explorations.api-test
  (:require
   [clojure.test :refer :all]
   [java-time.api :as t]
   [metabase.collections.models.collection :as collection]
   [metabase.permissions.core :as perms]
   [metabase.permissions.models.permissions-group :as perms-group]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]
   [toucan2.core :as t2]))

(use-fixtures :once (fixtures/initialize :db :web-server :test-users))

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
