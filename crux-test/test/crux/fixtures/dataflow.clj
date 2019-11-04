(ns crux.fixtures.dataflow
  (:require [clojure.test :refer :all]
            [crux.dataflow.schema :as schema]
            [crux.fixtures.api :as f]
            [crux.dataflow.api-2 :as dataflow])
  (:import (java.io Closeable)))

(def ^:dynamic ^Closeable *df-listener* nil)

(def task-schema
  {:task/owner [:Eid]
   :task/title [:String]
   :task/content [:String]
   :task/followers [:Eid ::schema/set]})

(def user-schema
  {:user/name [:String]
   :user/email [:String]
   :user/knows [:Eid ::schema/set]
   :user/likes [:String ::schema/list]})

(def full-schema
  (schema/calc-full-schema {:user user-schema, :task task-schema}))

(defn with-df-tx-listener [f]
  (with-open [listener
              (dataflow/start-dataflow-tx-listener f/*api*
                {:crux.dataflow/schema full-schema
                 :crux.dataflow/debug-connection? true
                 :crux.dataflow/embed-server?     true})]

    (binding [*df-listener* listener]
      (f))))
