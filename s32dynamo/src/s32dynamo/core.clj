(ns s32dynamo.core
  (:use [amazonica.aws.s3]
        [amazonica.aws.s3transfer]
        [clojure.data.json :as json :refer [write-str read-str]]
        [clojure.core.async :as async :refer [>! <! >!! <!! go]]
        [amazonica.aws.dynamodbv2 :as dynamo])
  (:gen-class))

(require '[metrics.core :refer [default-registry]])
(require '[metrics.counters :refer [counter defcounter inc! value]])
(require '[metrics.reporters.console :as console])
(def CR (console/reporter {}))

(defcounter default-registry writes-performed)

(def cred {:endpoint   "ap-southeast-2"})
(defn s3obj
  []
  (:object-content (get-object "andrewbrownepackages" "test2.json.gz")))

(defn jsonLines
  [contentStream]
  (->
   contentStream
   java.util.zip.GZIPInputStream.
   java.io.InputStreamReader.
   java.io.BufferedReader.
   line-seq
  ))

(defn recommendations
  []
  (clojure.core/map (fn [blah] {:jobId (rand-int 2000000) :score (str "0." (rand-int 9999))}) (range 50)))

(defn entries
  []
  (lazy-seq (clojure.core/map (fn [loginId] {:loginId loginId :batchId "1123432" :date "2012-04-23T18:25:43.511Z" :recommendations (recommendations)}) (range 1000000))))

(defn write-test-file
  []
  (with-open [wrtr (-> "test2.json.gz"
                       clojure.java.io/output-stream
                       java.util.zip.GZIPOutputStream.
                       clojure.java.io/writer)]
    (doseq [x ( entries )]
      (.write wrtr (json/write-str x))
      (.newLine wrtr))))

(defn toPutRequest
  [jsonStr]
  (let [item (json/read-str jsonStr)]
  {:put-request
   {:item {:loginId (str (get item "loginId"))
           :body (json/write-str item)
           }}}))

(defn writeItemsToCounter
  [& rest]
  (inc! writes-performed)
)

(defn writeItemsToDynamo
  [items]
  (dynamo/batch-write-item
   cred
   :request-items {"MyTable" items}))

(defn putDynamoItems
  [writeItems items]
  (writeItems
   (clojure.core/map toPutRequest items)))

(def in-chan (async/chan))
(def out-chan (async/chan))

(defn start-async-consumers
  "Start num-consumers threads that will consume work
  from the in-chan and put the results into the out-chan."
  [writeItems num-consumers]
  (dotimes [_ num-consumers]
    (async/thread
      (do
        (println "starting consumer")
      (while true
        (let [line (async/<!! in-chan)
              data (putDynamoItems writeItems line)]
          (async/>!! out-chan data)))
      ))))

(defn start-async-aggregator
  "Take items from the out-chan and print it."
  []
  (async/thread
    (while true
      (let [data (async/<!! out-chan)]
        (do)))))

(defn run
  [contentStream writeItems]
  (do
    (start-async-consumers writeItems 8)
    (start-async-aggregator)
    (doseq [x (clojure.core/partition 25 (jsonLines contentStream))]
     (async/>!! in-chan x))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
