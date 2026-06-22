(ns metabase.explorations.models.exploration-test
  (:require
   [clojure.test :refer :all]
   [metabase.collections.models.collection :as collection]
   [metabase.models.interface :as mi]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(deftest exploration-creates-entity-id-test
  (mt/with-temp [:model/User u {}
                 :model/Exploration e {:name "x" :creator_id (:id u)}]
    (is (= 21 (count (:entity_id e))))
    (testing "timestamps are populated"
      (is (some? (:created_at e)))
      (is (some? (:updated_at e))))))

(deftest exploration-collection-id-test
  (testing "omitting :collection_id leaves the exploration in the root collection"
    (mt/with-temp [:model/User u {}
                   :model/Exploration e {:name "x" :creator_id (:id u)}]
      (is (nil? (:collection_id e)))))
  (testing "explicit :collection_id is respected (including nil for root)"
    (mt/with-temp [:model/User       u {}
                   :model/Collection c {}
                   :model/Exploration explicit  {:name "y" :creator_id (:id u) :collection_id (:id c)}
                   :model/Exploration root-expl {:name "z" :creator_id (:id u) :collection_id nil}]
      (is (= (:id c) (:collection_id explicit)))
      (is (nil? (:collection_id root-expl))))))

(deftest exploration-in-personal-collection-is-creator-only-test
  (testing "an exploration in the creator's Personal Collection is private to creator + admins"
    (mt/with-temp [:model/User owner {}
                   :model/User other {}
                   :model/Exploration e {:name          "x"
                                         :creator_id    (:id owner)
                                         :collection_id (:id (collection/user->personal-collection (:id owner)))}]
      (testing "owner can read and write"
        (mt/with-current-user (:id owner)
          (is (true? (mi/can-read?  :model/Exploration (:id e))))
          (is (true? (mi/can-write? :model/Exploration (:id e))))))
      (testing "other user cannot read or write"
        (mt/with-current-user (:id other)
          (is (false? (mi/can-read?  :model/Exploration (:id e))))
          (is (false? (mi/can-write? :model/Exploration (:id e))))))
      (testing "superuser can read and write"
        (mt/with-test-user :crowberto
          (is (true? (mi/can-read?  :model/Exploration (:id e))))
          (is (true? (mi/can-write? :model/Exploration (:id e)))))))))

(deftest exploration-thread-perms-delegate-to-exploration-test
  (mt/with-temp [:model/User owner {}
                 :model/User other {}
                 :model/Exploration e {:name          "x"
                                       :creator_id    (:id owner)
                                       :collection_id (:id (collection/user->personal-collection (:id owner)))}
                 :model/ExplorationThread t {:exploration_id (:id e)}]
    (mt/with-current-user (:id owner)
      (is (true? (mi/can-read? :model/ExplorationThread (:id t)))))
    (mt/with-current-user (:id other)
      (is (false? (mi/can-read? :model/ExplorationThread (:id t)))))))

(deftest hydrate-threads-on-exploration-test
  (mt/with-temp [:model/User u {}
                 :model/Exploration e {:name "x" :creator_id (:id u)}
                 :model/ExplorationThread _t1 {:exploration_id (:id e) :position 0}
                 :model/ExplorationThread _t2 {:exploration_id (:id e) :position 1}]
    (let [hydrated (t2/hydrate (t2/select-one :model/Exploration :id (:id e)) :threads)]
      (is (= 2 (count (:threads hydrated))))
      (is (= [0 1] (mapv :position (:threads hydrated)))))))
