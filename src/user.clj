(ns user
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [criterium.core :as c]
            [core :refer [build-prepared-schema defresolver execute-query]]))

(def data (-> (io/resource "data.edn")
              slurp
              edn/read-string))

(defn- ->entity
  [data entity-name]
  (->> (get data entity-name)
       (reduce #(assoc %1 (:id %2) %2) {})))

(defn- ->reverse-entity
  [data entity-name field-name]
  (let [group-by-author (->> (get data entity-name)
                             (map (fn [root]
                                    (map (fn [field]
                                           [field root])
                                         (get root field-name))))
                             (apply concat)
                             (group-by first))]
    (-> group-by-author
        (update-vals (fn [books]
                       (map second books))))))

(def authors (->entity data :authors))
(def books-by-author (->reverse-entity data :books :authors))

(defresolver :Query/books
  [_ctx _args _parent]
  (:books data))

(defresolver :Book/authors
  "A book must have one or more authors."
  {:batch {:parent [:authors]}}
  [_ctx batch-args]
  (->> (map :authors batch-args)
       (map (fn [author-ids]
              (map #(get authors %)
                   author-ids)))))

(defresolver :Author/books
  {:batch {:parent [:id]}}
  [_ctx batch-args]
  (->> (map :id batch-args)
       (map #(get books-by-author %))))

(def prepared-schema (->> (io/resource "schema")
                          io/file
                          file-seq
                          (filter (fn [f] (.isFile f)))
                          (map slurp)
                          build-prepared-schema))

(defn execute-sample-query []
  (execute-query
   prepared-schema
   "query {
      books {
        ...BookFragment
      }
    }

    fragment BookFragment on Book {
      id
      title
     subject
     published
     authors {
       ...AuthorFragment
      }
    }

    fragment AuthorFragment on Author {
      id
      firstName
      lastName
      from
      until
      books {
        ...AuthorBookFragment
      }
    }

    fragment AuthorBookFragment on Book {
      id
      title
      subject
      published
    }"
   {:foo :bar}))

(comment
  (c/with-progress-reporting
    (c/bench (execute-sample-query)))

  (printf
   (execute-sample-query)))