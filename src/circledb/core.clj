(ns circledb.core
  (:require [clojure.set :as cset]
            [circledb.db :as db]
            [circledb.storage :as storage]))

(defn evolution-of [db ent-id attr-name]
  (loop [res []
         ts  (:curr-time db)]
    (if (= -1 ts)
      (reverse res)
      (let [attr (db/attr-at db ent-id attr-name ts)]
        (recur (conj res {(:ts attr) (:value attr)}) (:prev-ts attr))))))

(defn- next-ts [db]
  (inc (:curr-time db)))

(defn- update-creation-ts [ent ts-val]
  (reduce #(assoc-in %1 [:attrs %2 :ts] ts-val)
          ent
          (keys (:attrs ent))))

(defn- next-id [db ent]
  (let [top-id (:top-id db)
        ent-id (:id ent)
        new-id (inc top-id)]
    (if (= :db/no-id-yet ent-id)
      [(keyword (str new-id)) new-id]
      [ent-id top-id])))

(defn- fix-new-entity [db ent]
  (let [[ent-id next-top-id] (next-id db ent)
        new-ts (next-ts db)]
    [(update-creation-ts (assoc ent :id ent-id) new-ts) next-top-id]))

(defn- update-entry-in-index [index path operation]
  (let [update-path (butlast path)
        update-val  (last path)
        to-be-updated-set (get-in index update-path #{})]
    (assoc-in index update-path (conj to-be-updated-set update-val))))

(defn- update-attr-in-index [index ent-id attr-name target-val operation]
  (let [colled-target-val (db/collify target-val)
        update-entry-fn   (fn [i val]
                            (update-entry-in-index
                             i
                             ((db/from-eav index) ent-id attr-name val)
                             operation))]
    (reduce update-entry-fn index colled-target-val)))

(defn- add-entity-to-index [ent layer index-name]
  (let [ent-id          (:id ent)
        index           (index-name layer)
        all-attrs       (vals (:attrs ent))
        relevant-attrs  (filter #((db/usage-pred index) %) all-attrs)
        add-in-index-fn (fn [i attr]
                          (let [{:keys [name value]} attr]
                            (update-attr-in-index i
                                                  ent-id
                                                  name value
                                                  :db/add)))]
    (assoc layer index-name (reduce add-in-index-fn index relevant-attrs))))

(defn add-entity [db ent]
  (let [[fixed-ent next-top-id] (fix-new-entity db ent)
        new-storage-layer (update-in (last (:layers db))
                                     [:storage]
                                     storage/write-entity
                                     fixed-ent)
        add-fn            (partial add-entity-to-index fixed-ent)
        new-layer         (reduce add-fn new-storage-layer (db/indexes))]
    (assoc db
           :layers (conj (:layers db) new-layer)
           :top-id next-top-id)))

(defn add-entities [db ents-seq]
  (reduce add-entity db ents-seq))

(defn- update-attr-modification-time [attr new-ts]
  (assoc attr :ts new-ts :prev-s (:ts attr)))

(defn- update-attr-value [attr val operation]
  (if (db/single? attr)
    (assoc attr :value #{val})
    (case operation
      :db/reset-to (assoc attr :value val)
      :db/add      (assoc attr :value (cset/union (:value attr) val))
      :db/remove   (assoc attr :value (cset/difference (:value attr) val)))))

(defn- update-attr [attr new-val new-ts operation]
  {:pre [(if (db/single? attr)
           (contains? #{:db/reset-to :db/remove} operation)
           (contains? #{:db/reset-to :db/add :db/remove} operation))]}
  (-> attr
      (update-attr-modification-time new-ts)
      (update-attr-value new-val operation)))

(defn- put-entity [storage ent-id new-attr]
  (assoc-in (storage/get-entity storage ent-id)
            [:attrs (:name new-attr)]
            new-attr))

(defn- remove-entry-from-index [index path]
  (let [path-head       (first path)
        path-to-items   (butlast path)
        val-to-remove   (last path)
        old-entries-set (get-in index path-to-items)]
    (cond
      (not (contains? old-entries-set val-to-remove))
      index

      (= 1 (count old-entries-set))
      (update-in index [path-head] dissoc (second path))

      :else
      (update-in index path-to-items disj val-to-remove))))

(defn- remove-entries-from-index [ent-id operation index attr]
  (if (= :db/add operation)
    index
    (let [attr-name  (:name attr)
          datom-vals (db/collify (:value attr))
          paths      (map #((db/from-eav index) ent-id attr-name %)
                          datom-vals)]
      (reduce remove-entry-from-index index paths))))

(defn- update-index [ent-id old-attr new-val operation layer index-name]
  (if-not ((db/usage-pred (get-in layer [index-name])) old-attr)
    layer
    (let [index (index-name layer)
          cleaned-index (remove-entries-from-index ent-id
                                                   operation
                                                   index
                                                   old-attr)
          updated-index (if (= :db/remove operation)
                          cleaned-index
                          (update-attr-in-index cleaned-index
                                                ent-id
                                                (:name old-attr)
                                                new-val
                                                operation))]
      (assoc layer index-name updated-index))))

(defn- update-layer [layer ent-id old-attr new-attr new-val operation]
  (let [storage (:storage layer)
        new-layer (reduce
                   (partial update-index ent-id old-attr new-val operation)
                   layer
                   (db/indexes))]
    (assoc new-layer
           :storage
           (storage/write-entity storage
                                 (put-entity storage ent-id new-attr)))))

(defn update-entity
  ([db ent-id attr-name new-val]
   (update-entity db ent-id attr-name new-val :db/reset-to))
  ([db ent-id attr-name new-val operation]
   (let [update-ts     (next-ts db)
         layer         (last (:layers db))
         attr          (db/attr-at db ent-id attr-name)
         updated-attr  (update-attr attr new-val update-ts operation)
         updated-layer (update-layer layer
                                     ent-id
                                     attr
                                     updated-attr
                                     new-val
                                     operation)]
     (update-in db [:layers] conj updated-layer))))

(defn remove-entity-from-index [ent layer index-name]
  (let [ent-id           (:id ent)
        index            (index-name layer)
        attrs            (vals (:attrs ent))
        relevant-attrs   (filter #((db/usage-pred index) %)
                                 attrs)
        rm-from-index-fn (partial remove-entries-from-index
                                  ent-id
                                  :db/remove)]
    (assoc layer index-name (reduce rm-from-index-fn index relevant-attrs))))

(defn- reffing-to [ent-id layer]
  (let [vaet (:VAET layer)]
    (for [[attr-name reffing-set] (ent-id vaet)
          reffing reffing-set]
      [reffing attr-name])))

(defn- remove-back-refs [db ent-id layer]
  (let [reffing-datoms (reffing-to ent-id layer)
        remove-fn      (fn [_d [e a]]
                         (update-entity db e a ent-id :db/remove))
        clean-db       (reduce remove-fn db reffing-datoms)]
    (last (:layers clean-db))))

(defn remove-entity [db ent-id]
  (let [ent (db/entity-at db ent-id)
        layer (remove-back-refs db ent-id (last (:layers db)))
        no-ref-layer (update-in layer [:VAET] dissoc ent-id)
        no-ent-layer (assoc no-ref-layer
                            :storage
                            (storage/drop-entity (:storage no-ref-layer) ent))
        new-layer (reduce (partial remove-entity-from-index ent)
                          no-ent-layer (db/indexes))]
    (assoc db :layers (conj (:layers db) new-layer))))

(defn transact-on-db [initial-db ops]
  (loop [[op & more] ops
         res initial-db]
    (if op
      (recur more (apply (first op) res (rest op)))
      (let [initial-layer (:layers initial-db)
            new-layer     (last (:layers res))]
        (assoc initial-db
               :layers (conj initial-layer new-layer)
               :curr-time (next-ts initial-db)
               :top-id (:top-id res))))))

(defn- _what-if [db f txs]
  (f db txs))

(defmacro _transact [db op & txs]
  (when txs
    (loop [[first-tx# & rest-tx#] txs
           res#     [op db `transact-on-db]
           acc-txs# []]
      (if first-tx#
        (recur rest-tx# res# (conj acc-txs# (vec first-tx#)))
        (list* (conj res# acc-txs#))))))

(defmacro transact [db-conn & txs]
  `(_transact ~db-conn swap! ~@txs))

(defmacro what-if [db-conn & txs]
  `(_transact ~db-conn _what-if ~@txs))

(defn -main [& _args]
  (println "hi world"))

(comment
  (let [d    (db/make-db)
        ent1 (-> (db/make-entity 167)
                 (db/add-attr
                  (db/make-attr :person/name "James Cameron" "string")))
        ent2 (-> (db/make-entity 234)
                 (db/add-attr
                  (db/make-attr :movie/title "Die Hard" "string"))
                 (db/add-attr
                  (db/make-attr :movie/year 1987 "integer")))
        ent3 (-> (db/make-entity 235)
                 (db/add-attr
                  (db/make-attr :movie/title "Terminator" "string"))
                 (db/add-attr
                  (db/make-attr :movie/director 167 :db/ref)))]
    (transact d
              (add-entity ent1)
              (add-entity ent2)
              (add-entity ent3))
    #_(circledb.query/q @d
                        {:find  [?e]
                         :where [[?e :movie/year 1987]]})
    (circledb.query/q @d
                      {:find  [?e ?title]
                       :where [[?e :movie/title ?title]]})
    #_(circledb.query/q @d
                        {:find  [?name]
                         :where [[?m :movie/title "Robocop"]
                                 [?m :movie/director ?d]
                                 [?d :person/name ?name]]})))
