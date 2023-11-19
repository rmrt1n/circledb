(ns circledb.core-test
  (:require [clojure.test :refer [deftest testing is]]
            [circledb.db :as db]
            [circledb.storage :as storage]
            [circledb.core :as cdb]))

(defn base-layer []
  (db/map->Layer {:storage (storage/->InMemory)
                  :VAET {} :AVET {} :VEAT {} :EAVT {}}))

(deftest add-entity
  (testing "add-entity no empty entity to empty db"
    (let [d   (db/make-db)
          ent (db/make-entity)]
      (is (= (db/map->Database
              {:layers [(base-layer)
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {:1 (db/map->Entity
                                         {:id :1
                                          :attrs {}})})
                          :VAET {} :AVET {} :VEAT {} :EAVT {}})]
               :top-id 1
               :curr-time 0})
             (cdb/add-entity @d ent)))))
  (testing "add-entity entity with id to empty db"
    (let [d   (db/make-db)
          ent (db/make-entity 1)]
      (is (= (db/map->Database
              {:layers [(base-layer)
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {1 (db/map->Entity
                                        {:id 1
                                         :attrs {}})})
                          :VAET {} :AVET {} :VEAT {} :EAVT {}})]
               :top-id 0
               :curr-time 0})
             (cdb/add-entity @d ent))))) (testing "add-entity entity with id and attr to empty db"
                                           (let [d    (db/make-db)
                                                 ent  (db/make-entity 1)
                                                 attr (db/make-attr :user/name "ryan" :db.type/string)
                                                 ent  (db/add-attr ent attr)]
                                             (is (= (db/map->Database
                                                     {:layers [(base-layer)
                                                               (db/map->Layer
                                                                {:storage (storage/map->InMemory
                                                                           {1 (db/map->Entity
                                                                               {:id 1
                                                                                :attrs {:user/name
                                                                                        (db/map->Attr
                                                                                         {:name :user/name
                                                                                          :value "ryan"
                                                                                          :ts 1
                                                                                          :prev-ts -1})}})})
                                                                 :VAET {}
                                                                 :AVET {:user/name {"ryan" #{1}}}
                                                                 :VEAT {"ryan" {1 #{:user/name}}}
                                                                 :EAVT {1 {:user/name #{"ryan"}}}})]
                                                      :top-id 0
                                                      :curr-time 0})
                                                    (cdb/add-entity @d ent)))))
  (testing "add-entity multiple entities (add-entities)"
    (let [d     (db/make-db)
          ent1  (db/make-entity)
          ent2  (db/make-entity)
          ent3  (db/add-attr (db/make-entity :random-id)
                             (db/make-attr :user/name "ryan" :db.type/string))
          new-d (-> @d
                    (cdb/add-entity ent1)
                    (cdb/add-entity ent2)
                    (cdb/add-entity ent3))]
      (is (= (db/map->Database
              {:layers [(base-layer)
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {:1 (db/map->Entity
                                         {:id :1
                                          :attrs {}})})
                          :VAET {} :AVET {} :VEAT {} :EAVT {}})
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {:1 (db/map->Entity
                                         {:id :1
                                          :attrs {}})
                                     :2 (db/map->Entity
                                         {:id :2
                                          :attrs {}})})
                          :VAET {} :AVET {} :VEAT {} :EAVT {}})
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {:1 (db/map->Entity
                                         {:id :1
                                          :attrs {}})
                                     :2 (db/map->Entity
                                         {:id :2
                                          :attrs {}})
                                     :random-id
                                     (db/map->Entity
                                      {:id :random-id
                                       :attrs {:user/name
                                               (db/map->Attr
                                                {:name :user/name
                                                 :value "ryan"
                                                 :ts 1
                                                 :prev-ts -1})}})})
                          :VAET {}
                          :AVET {:user/name {"ryan" #{:random-id}}}
                          :VEAT {"ryan" {:random-id #{:user/name}}}
                          :EAVT {:random-id {:user/name #{"ryan"}}}})]
               :top-id 2
               :curr-time 0})
             new-d
             (cdb/add-entities @d [ent1 ent2 ent3]))))))

(deftest update-entity
  (testing "update-entity"
    (let [d   (db/make-db)
          ent (-> (db/make-entity 1)
                  (db/add-attr
                   (db/make-attr :user/name "ryan" :db.type/string)))
          d   (cdb/add-entity @d ent)
          ;; update assumes entity is added through a tx, updating :curr-time
          d   (assoc d :curr-time 1)]
      (is (= (db/map->Database
              {:layers [(base-layer)
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {1 (db/map->Entity
                                        {:id 1
                                         :attrs {:user/name
                                                 (db/map->Attr
                                                  {:name :user/name
                                                   :value "ryan"
                                                   :ts 1
                                                   :prev-ts -1})}})})
                          :VAET {}
                          :AVET {:user/name {"ryan" #{1}}}
                          :VEAT {"ryan" {1 #{:user/name}}}
                          :EAVT {1 {:user/name #{"ryan"}}}})
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {1 (db/map->Entity
                                        {:id 1
                                         :attrs {:user/name
                                                 (db/map->Attr
                                                  {:name :user/name
                                                   :value "martin"
                                                   :ts 2
                                                   :prev-ts 1})}})})
                          :VAET {}
                          :AVET {:user/name {"martin" #{1}}}
                          :VEAT {"martin" {1 #{:user/name}}}
                          :EAVT {1 {:user/name #{"martin"}}}})]
               :top-id 0
               :curr-time 1})
             (cdb/update-entity d 1 :user/name "martin"))))))

(deftest delete-entity
  (testing "delete-entity normal entity"
    (let [d   (db/make-db)
          ent (-> (db/make-entity 1)
                  (db/add-attr
                   (db/make-attr :user/name "ryan" :db.type/string)))
          d   (cdb/add-entity @d ent)
          d   (assoc d :curr-time 1)]
      (is (= (db/map->Database
              {:layers [(base-layer)
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {1 (db/map->Entity
                                        {:id 1
                                         :attrs {:user/name
                                                 (db/map->Attr
                                                  {:name :user/name
                                                   :value "ryan"
                                                   :ts 1
                                                   :prev-ts -1})}})})
                          :VAET {}
                          :AVET {:user/name {"ryan" #{1}}}
                          :VEAT {"ryan" {1 #{:user/name}}}
                          :EAVT {1 {:user/name #{"ryan"}}}})
                        (base-layer)]
               :top-id 0
               :curr-time 1})
             (cdb/remove-entity d 1)))))
  (testing "delete-entity referenced entity"
    (let [d    (db/make-db)
          ent1 (-> (db/make-entity 1)
                   (db/add-attr
                    (db/make-attr :user/name "ryan" :db.type/string)))
          ent2 (-> (db/make-entity 2)
                   (db/add-attr
                    (db/make-attr :user/ref 1 :db/ref)))
          d    (-> @d
                   (cdb/add-entity ent1)
                   (cdb/add-entity ent2)
                   (assoc :curr-time 2))]
      (is (= (db/map->Database
              {:layers [(base-layer)
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {1 (db/map->Entity
                                        {:id 1
                                         :attrs {:user/name
                                                 (db/map->Attr
                                                  {:name :user/name
                                                   :value "ryan"
                                                   :ts 1
                                                   :prev-ts -1})}})})
                          :VAET {}
                          :AVET {:user/name {"ryan" #{1}}}
                          :VEAT {"ryan" {1 #{:user/name}}}
                          :EAVT {1 {:user/name #{"ryan"}}}})
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {1 (db/map->Entity
                                        {:id 1
                                         :attrs {:user/name
                                                 (db/map->Attr
                                                  {:name :user/name
                                                   :value "ryan"
                                                   :ts 1
                                                   :prev-ts -1})}})
                                     2 (db/map->Entity
                                        {:id 2
                                         :attrs {:user/ref
                                                 (db/map->Attr
                                                  {:name :user/ref
                                                   :value 1
                                                   :ts 1
                                                   :prev-ts -1})}})})
                          :VAET {1 {:user/ref #{2}}}
                          :AVET {:user/name {"ryan" #{1}}
                                 :user/ref {1 #{2}}}
                          :VEAT {"ryan" {1 #{:user/name}}
                                 1 {2 #{:user/ref}}}
                          :EAVT {1 {:user/name #{"ryan"}}
                                 2 {:user/ref #{1}}}})
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {1 (db/map->Entity
                                        {:id 1
                                         :attrs {:user/name
                                                 (db/map->Attr
                                                  {:name :user/name
                                                   :value "ryan"
                                                   :ts 1
                                                   :prev-ts -1})}})})
                          :VAET {}
                          :AVET {:user/name {"ryan" #{1}}}
                          :VEAT {"ryan" {1 #{:user/name}}}
                          :EAVT {1 {:user/name #{"ryan"}}}})]

               :top-id 0
               :curr-time 2})
             (cdb/remove-entity d 2))))))

(deftest transact
  (testing "transact multiple add-entity(s)"
    (let [d (db/make-db)
          ent1 (db/make-entity)
          ent2 (db/make-entity)]
      (is (= (db/map->Database
              {:layers [(base-layer)
                        (db/map->Layer
                         {:storage (storage/map->InMemory
                                    {:1 (db/map->Entity
                                         {:id :1
                                          :attrs {}})
                                     :2 (db/map->Entity
                                         {:id :2
                                          :attrs {}})})
                          :VAET {}
                          :AVET {}
                          :VEAT {}
                          :EAVT {}})]
               :top-id 2
               :curr-time 1})
             (cdb/transact d
                           (cdb/add-entity ent1)
                           (cdb/add-entity ent2)))))))
