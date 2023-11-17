(ns circledb.graph
  (:require [circledb.db :as db]))

(defn incoming-refs [db ts ent-id & ref-names]
  (let [vaet         (db/index-at db :VAET ts)
        all-attr-map (vaet ent-id)
        filtered-map (if (some? ref-names)
                       (select-keys ref-names all-attr-map)
                       all-attr-map)]
    (reduce into #{} (vals filtered-map))))

(defn outgoing-refs [db ts ent-id & ref-names]
  (let [val-filter-fn (if (some? ref-names)
                        #(vals (select-keys ref-names %))
                        vals)]
    (if-not ent-id
      []
      (->> (db/entity-at db ts ent-id)
           :attrs
           val-filter-fn
           (filter db/ref?)
           (mapcat :value)))))
