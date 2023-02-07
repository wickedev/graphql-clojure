(ns user
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [core :refer [build-prepared-schema defresolver execute-query
                          tag-with-type]]
            [criterium.core :as c]))

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
    (update-vals group-by-author #(map second %))))

(def authors (->entity data :authors))
(def books-by-author (->reverse-entity data :books :authors))

(defresolver :Query/books
  [_ctx {:keys [paging]} _parent]
  (let [first (get paging "first" 10)]
    (prn :first first)
    (->> (:books data)
         (take first)
         (map #(tag-with-type % :Book)))))

(defresolver :Book/authors
  "A book must have one or more authors."
  {:batch {:parent [:authors]}}
  [_ctx batch-args]
  (->> (map :authors batch-args)
       (map (fn [author-ids]
              (->> author-ids
                   (map #(get authors %))
                   (map #(tag-with-type % :Author)))))))

(defresolver :Author/books
  {:batch {:parent [:id]}}
  [_ctx batch-args]
  (->> (map :id batch-args)
       (map (fn [ids]
              (->> (get books-by-author ids)
                   (map #(tag-with-type % :Book)))))))

(def prepared-schema (->> (io/resource "schema")
                          io/file
                          file-seq
                          (filter (fn [f] (.isFile f)))
                          (map slurp)
                          build-prepared-schema))

(defn execute-sample-query [variables]
  (execute-query
   prepared-schema
   "query Books($first: Int, $after: String, $direction: Direction) {
      books(paging: {
        first: $first
        after: $after
        direction: $direction
      }) {
        __typename
        ...BookFragment
      }
    }

    fragment BookFragment on Book {
      __typename
      id
      title
      subject
      published
      authors {
        ...AuthorFragment
      }
    }
    
    fragment AuthorFragment on Author {
      __typename
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
      __typename
      id
      title
      subject
      published
    }"
   {:foo :bar} variables))

(comment
  (c/with-progress-reporting
    (c/bench (execute-sample-query {:first 10 :after nil :direction "DESC"})))

  (printf
   (execute-sample-query {:first 1 :after nil :direction "DESC"})))