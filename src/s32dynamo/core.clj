(ns s32dynamo.core
  (:use [amazonica.aws.s3]
        [amazonica.aws.s3transfer]
        [clojure.data.json :as json :refer [write-str read-str]]
        [clojure.core.async :as async :refer [>! <! >!! <!! go]]
        [amazonica.aws.dynamodbv2 :as dynamo])
  (:gen-class))

(def cred {:endpoint   "ap-southeast-2"})
(defn s3obj
  []
  (get-object "andrewbrownepackages" "test2.json.gz"))

(defn jsonLines
  []
  (->
   (s3obj)
   (:object-content)
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

(defn putDynamoItems
  [items]
  (dynamo/batch-write-item
      cred
      :request-items
      {"MyTable"
       (clojure.core/map toPutRequest items)
       }
      ))

(def in-chan (async/chan))
(def out-chan (async/chan))

(defn start-async-consumers
  "Start num-consumers threads that will consume work
  from the in-chan and put the results into the out-chan."
  [num-consumers]
  (dotimes [_ num-consumers]
    (async/thread
      (while true
        (let [line (async/<!! in-chan)
              data (putDynamoItems line)]
          (async/>!! out-chan data))))))

(defn start-async-aggregator
  "Take items from the out-chan and print it."
  []
  (async/thread
    (while true
      (let [data (async/<!! out-chan)]
        (print (str "."))))))

(defn run2
  []
  (doseq [x (clojure.core/partition 25 (jsonLines))]
    (async/>!! in-chan x)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
