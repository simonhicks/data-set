(ns data-set.core
  (:require [clojure.set :as set]))

;; FIXME
;; - move the sample code at the bottom into some tests
;; - replace map-val with a standard util library (james reaves has a good one)
;; - write documentation about +fn and -fn
;; - tidy stuff up into a properly public/private api
;; - cljs all the things!

;; TODO
;; - add `remove-aspect` (which should remove all the filters and groupings too)

(defn map-val [func coll]
  (into {} (map (fn [[k v]]
                  [k (func v)])
                coll)))

(defn- sort-preserving-select-keys
  "Exactly like select-keys, except it uses the same kind of sorted-map as the
  original when the original map is sorted"
  [map keyseq]
  (reduce #(assoc %1 %2 (map %2))
          (empty map)
          keyseq))

(defprotocol IAspect
  (filtered-items [_ filt])
  (with-ordering [_ comparefn])
  (lookup [_ item])
  (all-items [_])
  (create-filter [_ pred]))

(defrecord Aspect [cache]
  IAspect
  (filtered-items [_ filt]
    (vals (sort-preserving-select-keys cache filt)))

  (with-ordering [this comparefn]
    (assoc this :cache (into (sorted-map-by comparefn) cache)))

  (lookup [_ item]
    (get cache item))

  (all-items [this]
    (vals cache))

  (create-filter [_ pred]
    (->> cache
         (filter #(pred (second %)))
         (map first)
         (into #{}))))

(defprotocol IReduction
  (get-reduction [_])
  (update-reduction [_ new-items removed-items]))

(defrecord Reduction [acc +fn -fn]
  IReduction
  (get-reduction [_] acc)

  (update-reduction [this new-items removed-items]
    (assoc this :acc (as-> acc %
                       (reduce +fn % new-items)
                       (reduce -fn % removed-items)))))

(defprotocol IGrouping
  (get-groups [_])
  (update-groups [_ new-items removed-items]))

(defrecord Grouping
  [asp reducts +fn -fn init]
  IGrouping
  (get-groups [_] (map-val get-reduction reducts))

  (update-groups [this new-items removed-items]
    (let [grouped-new-items (group-by #(lookup asp %) new-items)
          grouped-removed-items (group-by #(lookup asp %) removed-items)
          updated-values (distinct (concat (keys grouped-new-items) (keys grouped-removed-items)))]
      (->> updated-values
           (map (fn [aval]
                  [aval (update-reduction (or (reducts aval)
                                              (Reduction. init +fn -fn))
                                          (grouped-new-items aval)
                                          (grouped-removed-items aval))]))
           (into {})
           (merge reducts)
           (assoc this :reducts)))))

(defprotocol IHasData
  (data [_]))

(defprotocol IHasAspects
  (add-aspect [_ aname func comparefn])
  (aspect [_ aname]))

(defprotocol IHasReductions
  (create-reduction [_ rname +fn -fn init])
  (reduction [_ rname]))

(defprotocol IHasFilters
  (combined-filter [_]
    "Return a filter (ie. a hash-set) that represents the combined effect of all the filters")
  (remove-filter [_ aname fname]
    "Return a copy with the filter associated with aname and named by fname removed.")
  (remove-aspect-filters [_ aname]
     "Return a copy with all the filters associated with the named aspect removed.")
  (remove-all-filters [_]
     "Return a copy with all the filters removed.")
  (apply-filter [_ aname fname pred]
    "Return a copy with the specified filter added."))

(defn- apply-new-filter-to-reductions-and-groupings [old-filter dstore]
  (let [new-filter (combined-filter dstore)
        new-items (set/difference new-filter old-filter)
        removed-items (set/difference old-filter new-filter)
        new-reductions (map-val #(update-reduction % new-items removed-items) (:reductions dstore))
        new-groupings (map-val (fn [asp-grps]
                                 (map-val #(update-groups % new-items removed-items) asp-grps))
                               (:groupings dstore))]
      (-> dstore
          (assoc :reductions new-reductions)
          (assoc :groupings new-groupings))))

(defprotocol IHasGroupings
  (create-grouping [_ aname gname +fn -fn init])
  (groups [_ aname gname]))

(defrecord DataSet [_data_ aspects filters reductions groupings]
  IHasData
  (data [this]
    (filter (combined-filter this) _data_))

  IHasAspects
  (add-aspect [this aname func comparefn]
    (let [cache (if (nil? comparefn) {} (sorted-map-by comparefn))
          new-aspect (->> _data_
                           (map #(vector % (func %)))
                           (into cache)
                           (Aspect.))]
      (assoc-in this [:aspects aname] new-aspect)))

  (aspect [this aname]
    (if-let [filt (combined-filter this)]
      (filtered-items (get aspects aname) filt)
      (all-items (get aspects aname))))

  IHasFilters
  (combined-filter [_]
    (if (or (empty? filters) ; => {}
            (empty? (apply concat (vals filters))))
      (apply hash-set _data_)
      (->> filters
           vals
           (mapcat vals)
           (apply set/intersection))))

  (remove-filter [this aname fname]
    (apply-new-filter-to-reductions-and-groupings
      (combined-filter this)
      (update-in this [:filters aname] dissoc fname)))

  (remove-aspect-filters [this aname]
    (apply-new-filter-to-reductions-and-groupings
      (combined-filter this)
      (assoc-in this [:filters aname] {})))

  (remove-all-filters [this]
    (apply-new-filter-to-reductions-and-groupings
      (combined-filter this)
      (assoc this :filters {})))

  (apply-filter [this aname fname pred]
    (let [a (get aspects aname)
          filt (create-filter a pred)]
      (apply-new-filter-to-reductions-and-groupings
        (combined-filter this)
        (update-in this [:filters aname] (fnil assoc {}) fname filt))))

  IHasReductions
  (create-reduction [this rname +fn -fn init]
    (let [r (update-reduction (Reduction. init +fn -fn) (data this) '())]
      (update-in this [:reductions] (fnil assoc {}) rname r)))

  (reduction [this rname]
    (get-reduction (get-in this [:reductions rname])))
  
  IHasGroupings
  (create-grouping [this aname gname +fn -fn init]
    (let [g (update-groups (Grouping. (get aspects aname) {} +fn -fn init)
                           (data this) '())]
      (update-in this [:groupings aname] (fnil assoc {}) gname g)))

  (groups [_ aname gname]
    (get-groups (get-in groupings [aname gname]))))

(defn data-set [coll]
  (DataSet. coll {} {} {} {}))

;; create a basic data-set
(-> (data-set (map #(hash-map :number %) (range 1 11)))
    (add-aspect :n    :number                             #(> (:number %1) (:number %2)))
    (add-aspect :size #(if (> (:number %) 5)
                         :big
                         :small)
                #(< (:number %1) (:number %2)))
    (atom)
    (->> (def numbers)))

;; basic checks
(data @numbers)
(aspect @numbers :size)
(aspect @numbers :n)

;; applying a filter
(swap! numbers apply-filter :n :odd odd?)
(data @numbers)
(aspect @numbers :size)
(aspect @numbers :n)

;; creating a grouping
(swap! numbers create-grouping :size :sum #(+ %1 (:number %2)) #(- %1 (:number %2)) 0)
(groups @numbers :size :sum)

;; creating a reduction
(swap! numbers create-reduction :sum #(+ %1 (:number %2)) #(- %1 (:number %2)) 0)
(reduction @numbers :sum)

;; applying a second filter
(swap! numbers apply-filter :size :big #{:big})
(aspect @numbers :n)
(reduction @numbers :sum)
(groups @numbers :size :sum)

;; ... and removing it again
(swap! numbers remove-filter :size :big)
(aspect @numbers :n)
(reduction @numbers :sum)
(groups @numbers :size :sum)

;; removing the last filter
(swap! numbers remove-filter :n :odd)
(reduction @numbers :sum)
(groups @numbers :size :sum)
(aspect @numbers :n)

;; adding a different filter
(swap! numbers apply-filter :n :odd even?)
(reduction @numbers :sum)
(groups @numbers :size :sum)
(aspect @numbers :n)

