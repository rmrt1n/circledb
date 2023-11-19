(ns circledb.db-test
  (:require [clojure.test :refer [deftest testing is]]
            [circledb.db :as db]
            [circledb.storage :as storage]))

(deftest make-entity
  (testing "make-entity without id"
    (is (= (db/map->Entity {:id :db/no-id-yet
                            :attrs {}})
           (db/make-entity))))
  (testing "make-entity with id"
    (is (= (db/map->Entity {:id 1
                            :attrs {}})
           (db/make-entity 1)))))

(deftest make-attr
  (testing "make-attr of type long"
    (let [attr (db/make-attr :user/age 1 :db.type/long)]
      (is (= (db/map->Attr {:name :user/age
                            :value 1
                            :ts -1
                            :prev-ts -1})
             attr))
      (is (= {:type :db.type/long
              :cardinality :db.cardinality/one}
             (meta attr)))))
  (testing "make-attr of type string"
    (let [attr (db/make-attr :user/name "ryan" :db.type/string)]
      (is (= (db/map->Attr {:name :user/name
                            :value "ryan"
                            :ts -1
                            :prev-ts -1})
             attr))
      (is (= {:type :db.type/string
              :cardinality :db.cardinality/one}
             (meta attr)))))
  (testing "make-attr of type ref"
    (let [attr (db/make-attr :user/parent 1 :db.type/ref)]
      (is (= (db/map->Attr {:name :user/parent
                            :value 1
                            :ts -1
                            :prev-ts -1})
             attr))
      (is (= {:type :db.type/ref
              :cardinality :db.cardinality/one}
             (meta attr)))))
  (testing "make-attr of type tuple"
    (let [attr (db/make-attr :user/aliases
                             ["ll" "la" "lr"]
                             :db.type/tuple
                             {:cardinality :db.cardinality/many})]
      (is (= (db/map->Attr {:name :user/aliases
                            :value ["ll" "la" "lr"]
                            :ts -1
                            :prev-ts -1})
             attr))
      (is (= {:type :db.type/tuple
              :cardinality :db.cardinality/many}
             (meta attr))))))

(deftest add-attr
  (testing "add-attr"
    (let [ent  (db/make-entity 1)
          attr (db/make-attr :user/name "ryan" :db.type/string)]
      (is (= (db/map->Entity {:id 1
                              :attrs {:user/name
                                      (db/map->Attr
                                       {:name :user/name
                                        :value "ryan"
                                        :ts -1
                                        :prev-ts -1})}})
             (db/add-attr ent attr))))))

(deftest make-db
  (testing "make-db"
    (is (= (db/map->Database {:layers [(db/map->Layer
                                        {:storage (storage/->InMemory)
                                         :VAET {}
                                         :AVET {}
                                         :VEAT {}
                                         :EAVT {}})]
                              :top-id 0
                              :curr-time 0})
           @(db/make-db)))))

(deftest entity-at
  (testing "entity-at on an empty db"
    (let [d   (db/make-db)]
      (is (nil? (db/entity-at @d 1)))))
  (testing "entity-at on a db with correct entity"
    (let [d   (db/make-db)
          ent (db/make-entity 1)
          d   (assoc-in @d [:layers 0 :storage 1] ent)]
      (is (= ent
             (db/entity-at d 1)))
      (is (= ent
             (db/entity-at d 1 0))))))

;; TODO: test with 2 layers for each of these *-a fns
(deftest attr-at
  (testing "attr-at on an empty db"
    (let [d (db/make-db)]
      (is (nil? (db/attr-at @d 1 :user/name)))))
  (testing "attr-at on a db with correct entity"
    (let [d    (db/make-db)
          ent  (db/make-entity 1)
          attr (db/make-attr :user/name "ryan" :db.type/string)
          ent  (db/add-attr ent attr)
          d    (assoc-in @d [:layers 0 :storage 1] ent)]
      (is (= attr
             (db/attr-at d 1 :user/name))))))

(deftest value-of-at
  (testing "value-of-at on an empty db"
    (let [d (db/make-db)]
      (is (nil? (db/value-of-at @d 1 :user/name)))))
  (testing "value-of-at on a db with correct entity"
    (let [d    (db/make-db)
          ent  (db/make-entity 1)
          attr (db/make-attr :user/name "ryan" :db.type/string)
          ent  (db/add-attr ent attr)
          d    (assoc-in @d [:layers 0 :storage 1] ent)]
      (is (= "ryan"
             (db/value-of-at d 1 :user/name))))))

;; TODO: test indexes' meta
(deftest index-at
  (testing "index-at"
    (let [d (db/make-db)
          vaet (db/index-at @d :VAET)
          avet (db/index-at @d :AVET)
          veat (db/index-at @d :VEAT)
          eavt (db/index-at @d :EAVT)]
      (is (= {} vaet avet veat eavt)))))

(deftest collify
  (testing "collify on non coll"
    (is (= [1] (db/collify 1)))
    (is (= ["test"] (db/collify "test"))))
  (testing "collify on coll"
    (is (= [1] (db/collify [1])))
    (is (= ["test"] (db/collify ["test"])))))

(deftest single?
  (testing "single? on one"
    (is (db/single? (db/make-attr :user/name "ryan" :db.type/string))))
  (testing "single? on many"
    (is (not (db/single?
              (db/make-attr :user/aliases
                            ["hi"]
                            :db.type/tuple
                            {:cardinality :db.cardinality/many}))))))
