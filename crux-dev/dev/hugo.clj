(ns hugo
  (:require [clojure.java.io :as java-io]
            [clojure.java.shell :refer [sh]]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.namespace.repl :as tn]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.bootstrap :as b]
            [crux.standalone :as standalone]
            [crux.byte-utils :as bu]
            [crux.db :as db]
            [crux.api :as crux]
            [crux.index :as idx]
            [crux.kafka.embedded :as ek]
            [crux.kv :as kv]
            [crux.http-server :as srv]
            [crux.codec :as c]
            [crux.io :as crux-io]
            [crux.kafka :as k]
            [crux.memory :as mem]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [crux.jdbc :as jdbc]
            [crux.watdiv-test :as watdiv-test]
            [crux.hash :as hash])
  (:import [crux.api Crux ICruxAPI]
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory
           java.io.Closeable
           java.util.Date))

(def db-dir "data/h2-benchmark")

(defn delete-directory-recursive
  "Recursively delete a directory."
  [path]
  (let [^java.io.File file (java-io/file path)]
    (when (.exists file)
      (when (.isDirectory file)
        (doseq [file-in-dir (.listFiles file)]
          (delete-directory-recursive file-in-dir)))
      (java-io/delete-file file))))

(defn jdbc-test []
  (.setLevel
   ^Logger (org.slf4j.LoggerFactory/getLogger (Logger/ROOT_LOGGER_NAME))
   Level/ALL)
  (log/info "Starting JDBC test")
  (let [h2-node ^crux.api.ICruxAPI
        (crux/start-jdbc-node {:dbtype "h2"
                               :dbname "h2-benchmark-node"
                               :db-dir db-dir})]
    (try
      (let [_ (log/info (crux/status h2-node))
            results [(Date.)
                     (with-open
                       [in (-> watdiv-test/watdiv-triples-resource
                               java-io/resource
                               java-io/input-stream)]
                       (rdf/submit-ntriples (:tx-log h2-node) in 1000))
                     (Date.)]
            _ (log/info (crux/status h2-node))]
        results)
      (finally
        (.close h2-node)
        (delete-directory-recursive db-dir)))))
