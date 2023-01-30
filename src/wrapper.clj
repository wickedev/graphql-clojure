(ns wrapper
  (:require [util :as util])
  (:import [graphql.schema DataFetcher]
           [graphql.schema TypeResolver]
           [org.dataloader BatchLoaderWithContext]))

(deftype TypeResolverWrapper [get-type]
  TypeResolver
  (getType [_ env]
    (get-type env)))

(deftype UnaryOperatorWrapper [f]
  java.util.function.UnaryOperator
  (apply [_ v]
    (f v)))

(deftype BatchLoaderWithContextWrapper [f]
  BatchLoaderWithContext
  (load [_ keys ctx]
    (f keys ctx)))

(deftype DataFetcherWrapper [qualified-field option resolve-fn]
  DataFetcher
  (get [_this env]
    (let [dataloader (.getDataLoader env (str qualified-field))
          ctx (or (util/ctx->m (.getContext env)) {})
          args (or (util/jm->m (.getArguments env)) {})
          parent (or (util/jm->m (.getSource env)) {})]
      (if (and dataloader (:batch option))
        (.load dataloader (util/make-batch-arg option ctx args parent))
        (util/stringify-keys (resolve-fn ctx args parent))))))

(defrecord Resolver
           [qualified-field
            object-type
            field
            option
            datafetcher
            batchloader])