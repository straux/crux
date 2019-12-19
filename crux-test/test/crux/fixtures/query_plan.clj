(ns crux.fixtures.query-plan
  (:require [crux.fixtures.instrument :as instr]
            [crux.fixtures.trace :as t]
            [crux.index :as i]
            [loom.graph :as g]
            [loom.io :as lio]))

(comment
  (lio/view (g/add-nodes (g/graph) :a :b)))

(defn loom-instrumenter [g visited i]
  (swap! g g/add-nodes (t/index-name i))
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
