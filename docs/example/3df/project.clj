(defproject juxt/crux-3df "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core "19.09-1.4.0-alpha"]
                 [juxt/crux-rocksdb "19.09-1.4.0-alpha"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [com.sixthnormal/clj-3df "0.1.0-alpha"]]
  :global-vars {*warn-on-reflection* true})
