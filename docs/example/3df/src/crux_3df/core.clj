(ns crux-3df.core
  (:require
   [clojure.spec.alpha :as s]
   [clojure.tools.logging :as log]
   [clj-3df.core :as df]
   [clj-3df.attribute :as attribute]
   [crux.api :as api]
   [crux.bootstrap :as b]
   [crux.codec :as c]
   [crux.memory :as mem]
   [crux.standalone :as standalone])
  (:import java.io.Closeable
           java.nio.ByteBuffer))

;; TODO: This breaks range queries for longs.
(extend-protocol c/IdToBuffer
  Long
  (id->buffer [^Long this to]
    (c/id-function
     to (.array (doto (ByteBuffer/allocate Long/BYTES)
                  (.putLong this))))))

(defn validate-schema!
  [crux schema tx-ops]
  (doseq [[tx-op {:keys [crux.db/id] :as doc}] tx-ops]
    (case tx-op
      :crux.tx/put (do (assert (instance? Long id))
                       (assert (map? doc))
                       (doseq [[k v] (dissoc doc :crux.db/id)
                               :let [{:keys [db/valueType input_semantics]} (get schema k)]]
                         (assert (contains? schema k))
                         (case input_semantics
                           "CardinalityMany"
                           (case valueType
                             (:Eid :Number) (do (assert (coll? v))
                                                (doseq [i v]
                                                  (assert (number? i))))
                             :String (do (assert (coll? v))
                                         (doseq [i v]
                                           (assert (string? i)))))

                           "CardinalityOne"
                           (case valueType
                             (:Eid :Number) (assert (number? v))
                             :String (assert (string? v)))))))))

(defn index-to-3df
  [conn db crux {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id]}]
  (let [crux-db (api/db crux)]
    (with-open [snapshot (api/new-snapshot crux-db)]
      (let [new-transaction
            (reduce
             (fn [acc [op-key doc-or-id]]
               (case op-key
                 :crux.tx/put (let [new-doc doc-or-id
                                    _ (log/info "NEW-DOC:" new-doc)
                                    eid (:crux.db/id new-doc)
                                    old-doc (some->> (api/history-descending crux-db snapshot (:crux.db/id new-doc))
                                                     ;; NOTE: This comment seems like a potential bug?
                                                     ;; history-descending inconsistently includes the current document
                                                     ;; sometimes (on first transaction attleast
                                                     (filter
                                                      (fn [entry] (not= (:crux.tx/tx-id entry) tx-id)))
                                                     first :crux.db/doc)]
                                (into
                                 acc
                                 (apply
                                  concat
                                  (for [k (set (concat
                                                (keys new-doc)
                                                (keys old-doc)))
                                        :when (not= k :crux.db/id)]
                                    (let [old-val (get old-doc k)
                                          new-val (get new-doc k)
                                          old-set (when old-val (if (coll? old-val) (set old-val) #{old-val}))
                                          new-set (when new-val (if (coll? new-val) (set new-val) #{new-val}))]
                                      (log/info "KEY:" k old-set new-set)
                                      (concat
                                       (for [new new-set
                                             :when new
                                             :when (or (nil? old-set) (not (old-set new)))]
                                         [:db/add eid k new])
                                       (for [old old-set
                                             :when old
                                             :when (or (nil? new-set) (not (new-set old)))]
                                         [:db/retract eid k old])))))))))
             []
             tx-ops)]
        (log/info "3DF Tx:" new-transaction)
        @(df/exec! conn (df/transact db new-transaction))))))

(defn start-3df-connection [node {:keys [crux.3df/url]}]
  (df/create-debug-conn! url))

(defn start-3df-db [{:keys [conn-3df] :as node} {:keys [crux.3df/schema]}]
  (let [db (df/create-db schema)]
    (df/exec! conn-3df (df/create-db-inputs db))
    db))

(defn start-3df-tx-listener ^java.io.Closeable [{:keys [conn-3df db-3df] :as node}
                                                {:keys [crux.3df/poll-interval]
                                                 :or {poll-interval 100}}]
  ;; TODO: How should you really wrap an existing, started node?
  (let [crux (assoc (b/map->CruxNode node) :options {})
        worker-thread-3df (doto (Thread.
                                 ^Runnable (fn []
                                             (try
                                               (loop [tx-id -1]
                                                 (let [tx-id (with-open [tx-log-context (api/new-tx-log-context crux)]
                                                               (reduce
                                                                (fn [_ {:keys [crux.tx/tx-id] :as tx-log-entry}]
                                                                  (index-to-3df conn-3df db-3df crux tx-log-entry)
                                                                  tx-id)
                                                                tx-id
                                                                (api/tx-log crux tx-log-context (inc tx-id) true)))]
                                                   (Thread/sleep poll-interval)
                                                   (recur (long tx-id))))
                                               (catch InterruptedException ignore))))
                            (.setName "crux.3df.worker-thread")
                            (.start))]
    (reify Closeable
      (close [_]
        (doto worker-thread-3df
          (.interrupt)
          (.join))))))

(s/def :crux.3df/url string?)
(s/def :crux.3df/schema map?)
(s/def :crux.3df/poll-interval nat-int?)

(def conn-3df [start-3df-connection [] (s/keys :req [:crux.3df/url])])
(def db-3df [start-3df-db [:conn-3df] (s/keys :req [:crux.3df/schema])])
;; TODO: How should you really wrap an existing, started node? See
;; above as well.
(def tx-listener-3df [start-3df-tx-listener
                      [:conn-3df :db-3df :tx-log :kv-store :object-store :indexer]
                      (s/keys :opt [:crux.3df/poll-interval])])

(def node-config-3df {:conn-3df conn-3df
                      :db-3df db-3df
                      :tx-listener-3df tx-listener-3df})

(comment
  (def schema
    {:user/name (merge
                 (attribute/of-type :String)
                 (attribute/input-semantics :db.semantics.cardinality/one)
                 (attribute/tx-time))

     :user/email (merge
                  (attribute/of-type :String)
                  (attribute/input-semantics :db.semantics.cardinality/one)
                  (attribute/tx-time))

     :user/knows (merge
                  (attribute/of-type :Eid)
                  (attribute/input-semantics :db.semantics.cardinality/many)
                  (attribute/tx-time))

     :user/likes (merge
                  (attribute/of-type :String)
                  (attribute/input-semantics :db.semantics.cardinality/many)
                  (attribute/tx-time))})

  (def index-dir "data/db-dir")
  (def log-dir "data/eventlog")

  (def crux-options
    {:kv-backend "crux.kv.rocksdb.RocksKv"
     :bootstrap-servers "kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092"
     :event-log-dir log-dir
     :db-dir index-dir
     :crux.3df/url "ws://127.0.0.1:6262"
     :crux.3df/schema schema})

  (def ^Closeable crux (b/start-node (merge standalone/node-config node-config-3df) crux-options))

  (api/submit-tx
   crux
   [[:crux.tx/put
     {:crux.db/id 1
      :user/name "Patrik"
      :user/likes ["apples" "bananas"]
      :user/email "p@p.com"}]])

  (api/submit-tx
   crux
   [[:crux.tx/put
     {:crux.db/id 1
      :user/likes ["something new" "change this"]
      :user/name "Patrik"
      :user/knows [3]}]])

  (api/submit-tx
   crux
   [[:crux.tx/put
     {:crux.db/id 2
      :user/name "lars"
      :user/knows [3]}]
    [:crux.tx/put
     {:crux.db/id 3
      :user/name "henrik"
      :user/knows [4]}]])

  (df/exec! (:conn-3df crux)
            (df/query
             (:db-3df crux) "patrik-email"
             '[:find ?email
               :where
               [?patrik :user/name "Patrik"]
               [?patrik :user/email ?email]]))

  (df/exec! (:conn-3df crux)
            (df/query
             (:db-3df crux) "patrik-likes"
             '[:find ?likes
               :where
               [?patrik :user/name "Patrik"]
               [?patrik :user/likes ?likes]]))

  (df/exec! (:conn-3df crux)
            (df/query
             (:db-3df crux) "patrik-knows-1"
             '[:find ?knows
               :where
               [?patrik :user/name "Patrik"]
               [?patrik :user/knows ?knows]]))

  (df/exec! (:conn-3df crux)
            (df/query
             (:db-3df crux) "patrik-knows"
             '[:find ?user-name
               :where
               [?patrik :user/name "Patrik"]
               (trans-knows ?patrik ?knows)
               [?knows :user/name ?user-name]]
             '[[(trans-knows ?user ?knows)
                [?user :user/knows ?knows]]
               [(trans-knows ?user ?knows)
                [?user :user/knows ?knows-between]
                (trans-knows ?knows-between ?knows)]]))

  (df/listen!
   (:conn-3df crux)
   :key
   (fn [& data] (log/info "DATA: " data)))

  (df/listen-query!
   (:conn-3df crux)
   "patrik-knows"
   (fn [& message]
     (log/info "QUERY BACK: " message)))

  (.close crux))
