(ns crux.fixtures.instrument
  (:require [crux.index :as i]))

(defprotocol Instrument
  (instrument [i f visited]))

(defn inst [depth visited i]
  (instrument i depth visited))

(defn ->instrumented-index [i f visited]
  (let  [ii (or (and (instance? InstrumentedLayeredIndex i) i)
                (get @visited i)
                (let [ii (f visited i)]
                  (swap! visited assoc i ii)
                  ii))]
    (if (instance? crux.index.BinaryJoinLayeredVirtualIndex i)
      (assoc ii :name (:name i))
      ii)))

(extend-protocol Instrument
  crux.index.NAryConstrainingLayeredVirtualIndex
  (instrument [this f visited]
    (let [this (update this :n-ary-index (partial inst f visited))]
      (->instrumented-index this f visited)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this f visited]
    (let [this (update this :unary-join-indexes (fn [indexes] (map (partial inst f visited) indexes)))]
      (->instrumented-index this f visited)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this f visited]
    (or (get @visited this)
        (let [this (update this :indexes (fn [indexes] (map (partial inst f visited) indexes)))]
          (->instrumented-index this f visited))))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this f visited]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (doall (map (partial inst f visited) (.indexes state)))]
      (set! (.indexes state) [lhs rhs])
      (->instrumented-index this f visited)))

  crux.index.RelationVirtualIndex
  (instrument [^crux.index.RelationVirtualIndex this f visited]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (set! (.indexes state) (mapv (partial inst f visited) (.indexes state)))
      (->instrumented-index this f visited)))

  Object
  (instrument [this f visited]
    (->instrumented-index this f visited)))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrumented-layered-idx->seq [f idx]
  (original-layered-idx->seq (instrument idx f (atom {}))))
