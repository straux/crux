(ns crux.fixtures.query-plan
  (:require [crux.fixtures.instrument :as instr]
            [crux.fixtures.trace :as t]
            [crux.index :as i]
            [loom.graph :as g]
            [loom.io :as lio]))

(defprotocol Children
  (children [i]))

(extend-protocol Children
  crux.index.NAryConstrainingLayeredVirtualIndex
  (children [this]
    (:n-ary-index this))

  crux.index.NAryJoinLayeredVirtualIndex
  (children [this]
    (:unary-join-indexes this))

  crux.index.UnaryJoinVirtualIndex
  (children [this]
    (:indexes this))

  crux.index.BinaryJoinLayeredVirtualIndex
  (children [^crux.index.BinaryJoinLayeredVirtualIndex this]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)]
      (.indexes state)))

  crux.index.RelationVirtualIndex
  (children [^crux.index.RelationVirtualIndex this]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (.indexes state)))

  Object
  (children [this]
    nil))

(comment
  (lio/view (g/add-nodes (g/graph) :a :b)))

(defn loom-instrumenter [g visited i]
  (swap! g g/add-nodes (t/index-name i))
  (swap! g (fn [g] (->> i
                        children
                        (map t/index-name)
                        (map (partial vector (t/index-name i)))
                        (apply g/add-edges g))))
  i)

(defn instrument-layered-idx->seq [idx]
  (let [g (atom (g/graph))
        f (partial loom-instrumenter g)]
    (let [x (instr/instrumented-layered-idx->seq f idx)]
      (lio/view @g)
      x)))

(defmacro with-plan [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
