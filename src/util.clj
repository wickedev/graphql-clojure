(ns util
  (:require [clojure.java.data :as jd]
            [clojure.walk :as walk])
  (:import [com.google.common.base CaseFormat]
           [com.google.gson GsonBuilder]))

(def gson (.. (GsonBuilder.)
              setPrettyPrinting
              create))

(defn drop-nth [n coll]
  (keep-indexed #(when (not= %1 n) %2) coll))

(defn- camelCase->kebab-case [s]
  (.to CaseFormat/LOWER_CAMEL
       CaseFormat/LOWER_HYPHEN s))

(defn- kebab-case->camelCase [s]
  (.to CaseFormat/LOWER_HYPHEN
       CaseFormat/LOWER_CAMEL s))

(def ^:private memoize-camelCase->kebab-case
  (memoize camelCase->kebab-case))

(def ^:private memoize-kebab-case->camelCase
  (memoize kebab-case->camelCase))

(defn keywordize-keys
  "Recursively transforms all map keys from camelCase strings to kebab-case keywords."
  [m]
  (let [f (fn [[k v]]
            (if (string? k)
              [(-> (memoize-camelCase->kebab-case k)
                   keyword) v]
              [k v]))]
    ;; only apply to maps
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))


(defn stringify-keys
  "Recursively transforms all map keys from keywords to strings."
  {:added "1.1"}
  [m]
  (let [f (fn [[k v]]
            (if (keyword? k)
              [(-> (name k)
                   memoize-kebab-case->camelCase)
               v]
              [k v]))]
    ;; only apply to maps
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn make-batch-arg [{:keys [batch]} ctx args parent]
  (let [ctx' (select-keys ctx (:ctx batch))
        args' (select-keys args (:args batch))
        parent' (select-keys parent (:parent batch))]
    (merge ctx' args' parent')))

(defn ctx->m [ctx]
  (->> (.iterator (.stream ctx))
       iterator-seq
       (map (fn [eset]
              [(.getKey eset)
               (.getValue eset)]))
       (into {})))

(defn jm->m [jm]
  (-> (jd/from-java jm)
      keywordize-keys))
