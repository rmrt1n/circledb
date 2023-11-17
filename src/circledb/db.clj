(ns circledb.db
  (:require [circledb.storage :as storage]))

(defrecord Database [layers top-id curr-time])

(defrecord Layer [storage VAET AVET VEAT EAVT])

(defrecord Entity [id attrs])

(defrecord Attr [name value ts prev-ts])

(defn make-entity
  ([]
   (make-entity :db/no-id-yet))
  ([id]
   (Entity. id {})))

(defn make-attr
  ([name value type & {:keys [cardinality] :or {cardinality :db/single}}]
   {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
   (with-meta (Attr. name value -1 -1) {:type type :cardinality cardinality})))

(defn add-attr [ent attr]
  (let [attr-id (keyword (:name attr))]
    (assoc-in ent [:attrs attr-id] attr)))

(defn make-index [from-eav to-eav usage-pred]
  (with-meta {} {:from-eav from-eav :to-eav to-eav :usage-pred usage-pred}))

(defn from-eav [index]
  (:from-eav (meta index)))

(defn to-eav [index]
  (:to-eav (meta index)))

(defn usage-pred [index]
  (:usage-pred (meta index)))

(defn indexes []
  [:VAET :AVET :VEAT :EAVT])

(defn ref? [attr]
  (= :db/ref (:type (meta attr))))

(defn always [& _more]
  true)
(defn make-db []

  (atom
   (Database. [(Layer.
                (circledb.storage.InMemory.)
                (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) #(ref? %))
                (make-index #(vector %2 %3 %1) #(vector %2 %3 %1) always)
                (make-index #(vector %3 %1 %2) #(vector %3 %1 %2) always)
                (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) always))]
              0 0)))

(defn entity-at
  ([db ent-id]
   (entity-at db ent-id (:curr-time db)))
  ([db ent-id ts]
   (storage/get-entity (get-in db [:layers ts :storage])
                       ent-id)))

(defn attr-at
  ([db ent-id attr-name]
   (attr-at  ent-id attr-name (:curr-time db)))
  ([db ent-id attr-name ts]
   (get-in (entity-at db ent-id ts) [:attrs attr-name])))

(defn value-of-at
  ([db ent-id attr-name]
   (:value (attr-at db ent-id attr-name)))
  ([db ent-id attr-name ts]
   (:value (attr-at db ent-id attr-name ts))))

(defn index-at
  ([db kind]
   (index-at db kind (:curr-time db)))
  ([db kind ts]
   (kind ((:layers db) ts))))

(defn collify [x]
  (if (coll? x) x [x]))

(defn single? [attr]
  (= :db/single (:cardinality (meta attr))))

(comment
  (make-entity)
  (make-entity 1)
  (make-attr "name" "ryan" 'string)
  (let [ent (make-entity)
        attr (make-attr "name" "ryan" 'string)]
    (add-attr ent attr))
  (make-db))
