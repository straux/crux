(ns crux.dataflow-test
  (:require [clojure.test :refer :all]
            [clojure.test :as t]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.dataflow :as fdf]
            [crux.fixtures.api :as apif]))

(t/use-fixtures :each kvf/with-kv-dir fs/with-standalone-node apif/with-node fdf)
