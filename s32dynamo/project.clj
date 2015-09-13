(defproject s32dynamo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [amazonica "0.3.33"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [metrics-clojure "2.5.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [org.clojure/data.json "0.2.6"]]
  :main ^:skip-aot s32dynamo.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
