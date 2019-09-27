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
            [crux.hash :as hash]
            [oz.core :as oz])
  (:import [crux.api Crux ICruxAPI]
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory
           java.io.Closeable
           java.util.Date))

(defn reset []
  (require 'hugo :reload-all)
  :ok)

(defn delete-directory-recursive
  "Recursively delete a directory."
  [path]
  (let [^java.io.File file (java-io/file path)]
    (when (.exists file)
      (when (.isDirectory file)
        (doseq [file-in-dir (.listFiles file)]
          (delete-directory-recursive file-in-dir)))
      (java-io/delete-file file))))

(defn delete-test-fixtures [{:keys [dbname db-dir] :as options}]
  (delete-directory-recursive db-dir)
  (delete-directory-recursive (str dbname ".mv.db")))

(defn average [numbers]
  (if (empty? numbers)
    0
    (/ (reduce + numbers) (count numbers))))



(def storage-dir "bench-storage")
(def results-dir "bench-results")

(def ingested-file-locations
  {:watdiv-10M watdiv-test/watdiv-triples-resource
   :watdiv-100M watdiv-test/watdiv-100M
   :watdiv-1000M watdiv-test/watdiv-1000M})

(def default-options
  {:kafka {:crux.kafka.embedded/zookeeper-data-dir
           (str storage-dir "/kafka/zookeeper")
           :crux.kafka.embedded/kafka-log-dir
           (str storage-dir "/kafka/kafka-log")
           :crux.kafka.embedded/kafka-port 9092
           :db-dir (str storage-dir "/kafka/data")
           :bootstrap-servers "localhost:9092"
           :server-port 3000
           :bench/embed-kafka? true
           :bench/node-config k/node-config
           :bench/node-start-fn b/start-node
           :bench/log-level :debug
           :bench/storage :kafka
           :bench/ingested-file :watdiv-10M}

   :jdbc {:dbtype "h2"
          :dbname "h2-benchmark-node"
          :db-dir (str storage-dir "/h2/data")
          :server-port 3000
          :bench/node-start-fn (fn [_ options]
                                 (crux/start-jdbc-node options))
          :bench/log-level :debug
          :bench/storage :jdbc
          :bench/back-end :rocksdb
          :bench/storage-ns 'crux.jdbc
          :bench/ingested-file :watdiv-10M}})

#_[[nil = :standalone
    :kafka
    :embedded-kafka
    :jdbc [nil = :h2
           :postgresql :oracle :mysql :sqlite]]
   [nil = :rocksdb
    :memkv]
   [nil :http]]

(defn combine-with-default-options
  [user-options & ks]
  (apply merge (into (map ks default-options)
                     user-options)))

#_(def default-options-%%%%%%
    {:standalone
     {:event-log-dir (str storage-dir "/data/event-log-dir")}

     :kafka
     {:bootstrap-servers "localhost:9092"}

     :embedded-kafka
     {:crux.kafka.embedded/zookeeper-data-dir
      (str storage-dir "/data/zookeeper")
      :crux.kafka.embedded/kafka-log-dir
      (str storage-dir "/data/kafka-log")
      :crux.kafka.embedded/kafka-port 9092}

     :jdbc
     {:dbtype "h2" ;; "postgresql" "oracle" "mysql" "sqlite"
      :dbname "bench-jdbc"
      :host "localhost"
      :user "crux"
      :password "crux"}



     :rocksdb
     {:kv-backend "crux.kv.rocksdb.RocksKv"
      :db-dir (str storage-dir "/data/db-dir")}

     :memkv
     {:kv-backend "crux.kv.memdb.MemKv"
      :db-dir (str storage-dir "/data/db-dir")}

     :http
     {:server-port 3000}

     })

(defn summary [results]
  (let [start-date ^java.util.Date (:start-date (last results))
        end-date ^java.util.Date (:end-date (first results))
        duration-sec (/ (- (.getTime end-date)
                           (.getTime start-date))
                        1000.0)
        nb-facts (apply + (map :nb-facts results))
        fact-ingestion-times (map :avg-fact-ingestion-time results)]
    ;; All durations in seconds
    {:total-duration duration-sec
     :nb-facts nb-facts
     :avg-fact-ingestion-time (/ duration-sec nb-facts)
     :min-fact-ingestion-time (apply min fact-ingestion-times)
     :max-fact-ingestion-time (apply max fact-ingestion-times)}))

#_(defn average-fact-ingestion-time
    [{:keys [^java.util.Date start-date
             ^java.util.Date end-date
             nb-facts]
      :as entity-ingestion-result}]
    (let [duration (/ (- (.getTime end-date)
                         (.getTime start-date))
                      1000.0)]
      (/ duration nb-facts 1.0)))

#_(defn summary
    [results]
    (let [start-date ^java.util.Date (:start-date (last results))
          end-date ^java.util.Date (:end-date (first results))
          duration-sec (/ (- (.getTime end-date)
                             (.getTime start-date))
                          1000.0)
          nb-facts (apply + (map :nb-facts results))
          fact-ingestion-times (map average-fact-ingestion-time
                                    results)]
      ;; All durations in seconds
      {:total-duration duration-sec
       :nb-facts nb-facts
       :avg-fact-ingestion-time (/ duration-sec nb-facts)
       :min-fact-ingestion-time (apply min fact-ingestion-times)
       :max-fact-ingestion-time (apply max fact-ingestion-times)}))

#_(defn ingest-entity!
    [node entity]
    (let [tx-log (:tx-log node)
          nb-facts (count entity)
          start-date ^java.util.Date (Date.)
          _ (db/submit-tx tx-log [[:crux.tx/put entity]])
          end-date ^java.util.Date (Date.)]
      {:start-date start-date
       :end-date end-date
       :nb-facts nb-facts}))

#_ (defn ingest-entities!
     ([node entities]
      (doall (map (partial ingest-entity! node) entities)))
     ([node entities n]
      (ingest-entities! node (take n entities))))

#_(def ^ICruxAPI bench-node)
#_(defn start-node ^crux.api.ICruxAPI
    [{:bench/keys [embed-kafka? http-server?
                   node-config node-start-fn]
      :as options}]
    (let [started (atom [])]
      (try
        (let [embedded-kafka (when embed-kafka?
                               (doto (ek/start-embedded-kafka options)
                                 (->> (swap! started conj))))
              cluster-node (doto (node-start-fn node-config options)
                             (->> (swap! started conj)))
              http-server (when http-server?
                            (srv/start-http-server cluster-node options))]
          (assoc cluster-node
                 :http-server http-server
                 :embedded-kafka embedded-kafka))
        (catch Throwable t
          (doseq [c (reverse @started)]
            (crux-io/try-close c))
          (throw t)))))
#_(defn stop-node ^crux.api.ICruxAPI
    [{:keys [http-server embedded-kafka] :as node}]
    (doseq [c [http-server node embedded-kafka]]
      (crux-io/try-close c)))

(defn results-filepath
  [options]
  (str storage-dir
       "/" (name (:bench/storage options))
       "-" (name (:bench/back-end options))
       "-" (name (:bench/ingested-file options))))

(defn ingestion-bench
  ([] (ingestion-bench {:bench/storage :jdbc}))
  ([user-options]
   (if (keyword? user-options) ;; this is just for ease of dev since currently the storage choice is the biggest branch. will probably remove later down the line.
     (ingestion-bench {:bench/storage user-options})

     (let [{:keys [bench/storage-ns bench/log-level
                   bench/node-start-fn bench/node-config
                   bench/ingested-file
                   bench/nb-entities]
            :as options}
           (merge (get default-options (:bench/storage user-options))
                  user-options)
           ingested-file-location (get ingested-file-locations
                                       ingested-file)]
       ;; (with-log-level 'crux.bench.ingestion log-level
         ;; (with-log-level storage-ns log-level
           (try
             (with-open
               [bench-node ^crux.api.ICruxAPI
                (crux/start-jdbc-node options)
                in (-> ingested-file-location
                       java-io/resource
                       java-io/input-stream)]
               (log/info (str "Starting ingestion at " (Date.)))
               (log/info (str "using options " options))
               (let [results (rdf/submit-ntriples (:tx-log bench-node)
                                                  in 100000)]
                 (spit (results-filepath options) results)
                 results))
             (finally
               (delete-test-fixtures options)))
           ;; ))
       ))))

(defn ozify-result
  [^java.util.Date first-date
   {:keys [start-date end-date
           avg-fact-ingestion-time] :as result}]
  (let [time (/ (- (.getTime ^java.util.Date end-date)
                   (.getTime first-date))
                1000.0)]
    {:time time
     :item "jdbc"
     :fact-ingestion-time avg-fact-ingestion-time}))

(defn ozify
  [results]
  (let [first-date ^java.util.Date (:start-date (last results))
        values (map (partial ozify-result first-date) results)]
    {:data {:values values}
     :encoding {:x {:field "time"}
                :y {:field "fact-ingestion-time"}
                :color {:field "item" :type "nominal"}}
     :mark "line"}))
