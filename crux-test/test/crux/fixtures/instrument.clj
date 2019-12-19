(ns crux.fixtures.instrument
  (:require [crux.index :as i]))

(defprotocol Instrument
  (instrument [i f]))

(extend-protocol Instrument
  crux.index.NAryConstrainingLayeredVirtualIndex
  (instrument [this f]
    (let [this (update this :n-ary-index instrument f)]
      (f this)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this f]
    (let [this (update this :unary-join-indexes (fn [indexes] (doall (map #(instrument % f) indexes))))]
      (f this)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this f]
    (let [this (update this :indexes (fn [indexes] (doall (map #(instrument % f) indexes))))]
      (f this)))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this f]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (doall (map #(instrument % f) (.indexes state)))]
      (set! (.indexes state) [lhs rhs])
      (f this)))

  crux.index.RelationVirtualIndex
  (instrument [^crux.index.RelationVirtualIndex this f]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (set! (.indexes state) (mapv #(instrument % f) (.indexes state)))
      (f this)))

  Object
  (instrument [this f]
    (f this)))

(defn ->instrumented-index [visited f i]
  (let  [ii (or (get @visited i)
                (let [ii (f visited i)]
                  (swap! visited assoc i ii)
                  ii))]
    (if (instance? crux.index.BinaryJoinLayeredVirtualIndex i)
      (assoc ii :name (:name i))
      ii)))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrumented-layered-idx->seq [f idx]
  (let [f (partial ->instrumented-index (atom {}) f)]
    (original-layered-idx->seq (instrument idx f))))
