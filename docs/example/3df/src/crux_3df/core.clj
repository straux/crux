(ns crux-3df.core
  (:require
   [clojure.spec.alpha :as s]
   [clojure.tools.logging :as log]
   [clj-3df.core :as df]
   [clj-3df.attribute :as attribute]
   [manifold.stream]
   [crux.api :as api]
   [crux.bootstrap :as b]
   [crux.codec :as c]
   [crux.io :as cio]
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

(defrecord Crux3DFTxListener [conn db crux ^Thread worker-thread]
  Closeable
  (close [_]
    (manifold.stream/close! (:ws conn))
    (doto worker-thread
      (.interrupt)
      (.join))))

(s/def :crux.3df/url string?)
(s/def :crux.3df/schema map?)
(s/def :crux.3df/poll-interval nat-int?)

(s/def :crux.3df/tx-listener-options (s/keys :req [:crux.3df/url
                                                   :crux.3df/schema]
                                             :opt [:crux.3df/poll-interval]))

(defn start-crux-3df-tx-listener ^java.io.Closeable [crux-node
                                                     {:keys [crux.3df/url
                                                             crux.3df/schema
                                                             crux.3df/poll-interval]
                                                      :or {poll-interval 100}
                                                      :as options}]
  (s/assert :crux.3df/tx-listener-options options)
  (let [conn (df/create-debug-conn! url)
        db (df/create-db schema)]
    (df/exec! conn (df/create-db-inputs db))
    (let [worker-thread (doto (Thread.
                               ^Runnable (fn []
                                           (try
                                             (loop [tx-id -1]
                                               (let [tx-id (with-open [tx-log-context (api/new-tx-log-context crux-node)]
                                                             (reduce
                                                              (fn [_ {:keys [crux.tx/tx-id] :as tx-log-entry}]
                                                                (index-to-3df conn db crux-node tx-log-entry)
                                                                tx-id)
                                                              tx-id
                                                              (api/tx-log crux-node tx-log-context (inc tx-id) true)))]
                                                 (Thread/sleep poll-interval)
                                                 (recur (long tx-id))))
                                             (catch InterruptedException ignore))))
                          (.setName "crux.3df.worker-thread")
                          (.start))]
      (->Crux3DFTxListener conn db crux-node worker-thread))))

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

  (def ^Closeable crux (b/start-node standalone/node-config
                                     {:kv-backend "crux.kv.rocksdb.RocksKv"
                                      :event-log-dir "data/eventlog"
                                      :db-dir "data/db-dir"}))

  (def ^Closeable crux-3df (start-crux-3df-tx-listener
                            crux
                            {:crux.3df/url "ws://127.0.0.1:6262"
                             :crux.3df/schema schema}))

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

  (df/exec! (:conn crux-3df)
            (df/query
             (:db crux-3df) "patrik-email"
             '[:find ?email
               :where
               [?patrik :user/name "Patrik"]
               [?patrik :user/email ?email]]))

  (df/exec! (:conn crux-3df)
            (df/query
             (:db crux-3df) "patrik-likes"
             '[:find ?likes
               :where
               [?patrik :user/name "Patrik"]
               [?patrik :user/likes ?likes]]))

  (df/exec! (:conn crux-3df)
            (df/query
             (:db crux-3df) "patrik-knows-1"
             '[:find ?knows
               :where
               [?patrik :user/name "Patrik"]
               [?patrik :user/knows ?knows]]))

  (df/exec! (:conn crux-3df)
            (df/query
             (:db crux-3df) "patrik-knows"
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
   (:conn crux-3df)
   :key
   (fn [& data] (log/info "DATA: " data)))

  (df/listen-query!
   (:conn crux-3df)
   "patrik-knows"
   (fn [& message]
     (log/info "QUERY BACK: " message)))

  (doseq [c [crux-3df crux]]
    (cio/try-close c)))
