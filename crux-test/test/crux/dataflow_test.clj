(ns crux.dataflow-test
  (:require [clojure.test :refer :all]
            [clojure.test :as t]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.dataflow :as fdf]
            [crux.fixtures.api :as apif]
            [crux.dataflow.api-2 :as dataflow]
            [crux.api :as api])
  (:import (java.util.concurrent LinkedBlockingQueue TimeUnit)
           (java.time Duration)))

(t/use-fixtures :each
                fs/with-standalone-node
                kvf/with-kv-dir
                apif/with-node
                fdf/with-df-tx-listener)

(defn- submit-sync [txes]
  (let [tx-data (api/submit-tx apif/*api* txes)]
    (api/sync apif/*api* (Duration/ofSeconds 20))
    tx-data))

(t/deftest test-basic-query
  (submit-sync
    [[:crux.tx/put
      {:crux.db/id :ids/patrik
       :user/name  "pat"
       :user/email "pat@pat"}]])

  (let [sub ^LinkedBlockingQueue
        (dataflow/subscribe-query! fdf/*df-listener*
          {:crux.dataflow/sub-id ::four
           :crux.dataflow/query-name "user-with-eid"
           :crux.dataflow/results-shape :crux.dataflow.results-shape/maps
           :crux.dataflow/results-root-symbol '?user
           :crux.dataflow/query
           {:find ['?user '?name '?email]
            :where
            [['?user :user/name '?name]
             ['?user :user/email '?email]]}})]
    (submit-sync
      [[:crux.tx/put
        {:crux.db/id :ids/patrik
         :user/name  "pat2"
         :user/email "pat@pat2"}]])

    (let [res (.poll sub 1000 TimeUnit/MILLISECONDS)] ; 1000ms here is poll timeout, not interval
      (t/is res)
      (t/is "pat2" (get-in res [:updated-keys :ids/patrik :user/name])))))
