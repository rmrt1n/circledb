(ns circledb.query
  (:require
   [circledb.db :as db]
   [clojure.set :as cset]))

(defn- variable?
  ([x]
   (variable? x true))
  ([x accept]
   (or (and accept [= x "_"])
       (= (first x) \?))))

(defmacro symbol-col-to-set [coll]
  (set (map str coll)))

(defmacro clause-term-expr [clause-term]
  (cond
    (variable? (str clause-term))
    #(= % %)

    (not (coll? clause-term))
    `#(= % ~clause-term)

    (= 2 (count clause-term))
    `#(~(first clause-term) %)

    (variable? (str (second clause-term)))
    `#(~(first clause-term) % ~(last clause-term))

    (variable? (str (last clause-term)))
    `#(~(first clause-term) ~(second clause-term) %)))

(defmacro clause-term-meta [clause-term]
  (cond
    (coll? clause-term)
    (first (filter #(variable? % false)
                   (map str clause-term)))

    (variable? (str clause-term) false)
    (str clause-term)

    :else
    nil))

(defmacro pred-clause [clause]
  (loop [[trm# & rest-trm#] clause
         exprs# []
         metas# []]
    (if trm#
      (recur rest-trm#
             (conj exprs# `(clause-term-expr ~trm#))
             (conj metas# `(clause-term-meta ~trm#)))
      (with-meta exprs# {:db/variable metas#}))))

(defmacro q-clauses-to-pred-clauses [clauses]
  (loop [[first# & rest#] clauses
         pred-vecs# []]
    (if-not first#
      pred-vecs#
      (recur rest# `(conj ~pred-vecs# (pred-clause ~first#))))))

(defn index-of-joining-variable [query-clauses]
  (let [metas-seq     (map #(:db/variable (meta %))
                           query-clauses)
        collapsing-fn (fn [acc v]
                        (map #(when (= %1 %2) %1)
                             acc v))
        collapsed     (reduce collapsing-fn metas-seq)]
    (first (keep-indexed #(when (variable? %2 false) %1) collapsed))))

(defn filter-index [index pred-clauses]
  (for [pred-clause pred-clauses
        :let [[lvl1-pred lvl2-pred lvl3-pred]
              (apply (db/from-eav index) pred-clause)]
        [k1 l2map] index
        :when (try (lvl1-pred k1)
                   (catch Exception _e false))
        [k2 l3set] l2map
        :when (try (lvl2-pred k2)
                   (catch Exception _e false))
        :let [res (set (filter lvl3-pred l3set))]]
    (with-meta [k1 k2 res] (meta pred-clause))))

(defn items-answer-all-conditions [items-seq n-conditions]
  (->> items-seq
       (map vec)
       (reduce into [])
       frequencies
       (filter #(<= n-conditions (last %)))
       (map first)
       set))

(defn mask-path-leaf-with-items [relevant-items path]
  (update-in path [2] cset/intersection relevant-items))

(defn query-index [index pred-clause]
  (let [result-clauses  (filter-index index pred-clause)
        relevant-items  (items-answer-all-conditions (map last result-clauses)
                                                     (count pred-clause))
        cleaned-clauses (map (partial mask-path-leaf-with-items relevant-items)
                             result-clauses)]
    (filter #(not-empty (last %))
            cleaned-clauses)))

(defn combine-path-and-meta [from-eav-fn path]
  (let [expanded-path [(repeat (first path)) (repeat (second path)) (last path)]
        meta-of-path  (apply from-eav-fn (map repeat (:db/variable (meta path))))
        data-and-meta (interleave meta-of-path expanded-path)]
    (apply (partial map vector)
           data-and-meta)))

(defn bind-variables-to-query [q-res index]
  (let [seq-res-path (mapcat (partial combine-path-and-meta
                                      (db/from-eav index))
                             q-res)
        res-path     (map #(->> %
                                (partition 2)
                                (apply (db/to-eav index)))
                          seq-res-path)]
    (reduce #(assoc-in %1 (butlast %2) (last %2))
            {}
            res-path)))

(defn single-index-query-plan [query index db]
  (let [q-res (query-index (db/index-at db index) query)]
    (bind-variables-to-query q-res (db/index-at db index))))

(defn build-query-plan [query]
  (let [term-index (index-of-joining-variable query)
        index-to-use (case term-index
                       0 :AVET
                       1 :VEAT
                       2 :EAVT)]
    (partial single-index-query-plan query index-to-use)))

(defn resultify-bind-pair [vars-set acc pair]
  (let [[var-name _] pair]
    (if (contains? vars-set var-name)
      (conj acc pair)
      acc)))

(defn resultify-av-pair [vars-set acc av-pair]
  (reduce (partial resultify-bind-pair vars-set)
          acc
          av-pair))

(defn locate-vars-in-query-res [vars-set q-res]
  (let [[e-pair av-map] q-res
        e-res (resultify-bind-pair vars-set [] e-pair)]
    (map (partial resultify-av-pair vars-set e-res)
         av-map)))

(defn unify [binded-res-col needed-vars]
  (map (partial locate-vars-in-query-res needed-vars)
       binded-res-col))

(defmacro q [db query]
  `(let [pred-clause#        (q-clauses-to-pred-clauses ~(:where query))
         needed-vars#        (symbol-col-to-set ~(:find query))
         query-plan#         (build-query-plan pred-clause#)
         query-internal-res# (query-plan# ~db)]
     (unify query-internal-res# needed-vars#)))
