(ns user
  (:require [clojure.java.io :as io]
            [core :refer [build-prepared-schema defresolver execute-query]]))

(defresolver :Query/hello
  {:batch {:args [:input-num]}}
  [ctx batch-args]
  (prn :Query/hello :ctx ctx :batch-args batch-args)
  (map str batch-args))

(defresolver :Query/hi
  [ctx args parent]
  (prn :Query/hi :ctx ctx :args args :parent parent)
  "greeting")

(defresolver :Query/nested
  {:batch {}}
  [ctx batch-args]
  (prn :Query/nested ctx batch-args)
  (map (fn [_] {:id (random-uuid)}) batch-args))

(defresolver :Nested/nested
  {:batch {:parent [:id]}}
  [ctx batch-args]
  (prn :Nested/nested ctx batch-args)
  (map (fn [_] {:id (random-uuid)}) batch-args))

(defresolver :Nested/nesteds
  {:batch {:parent [:id]}}
  [ctx batch-args]
  (prn :Nested/nesteds ctx batch-args)
  (map (fn [_] [{:id (random-uuid)} {:id (random-uuid)}]) batch-args))

(def prepared-schema (->> (io/resource "schema")
                          io/file
                          file-seq
                          (filter (fn [f] (.isFile f)))
                          (map slurp)
                          build-prepared-schema))

(comment
  (printf
   (execute-query
    prepared-schema
    "query {
      aHi: hi
      bHi: hi
      aHello: hello(inputNum: 1)
      bHello: hello(inputNum: 2)
      cHello: hello(inputNum: 2)
      dHello: hello(inputNum: 2)
   }"
    {:foo 1}))

  (printf
   (execute-query
    prepared-schema
    "query {
      nested {
        id
        nested {
          id
        }
        nesteds {
          id
          nested {
            id
            nested {
              id
            }
          }
        }
      }
    }"
    {:bar 1})))