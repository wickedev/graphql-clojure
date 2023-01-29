(ns core
  (:require [clojure.java.data :as j]
            [clojure.walk :as walk]
            [promesa.core :as p])
  (:import [com.google.common.base CaseFormat]
           [com.google.gson GsonBuilder]
           [graphql GraphQL]
           [graphql ExecutionInput]
           [graphql.execution.instrumentation.dataloader DataLoaderDispatcherInstrumentation]
           [graphql.execution.instrumentation.dataloader DataLoaderDispatcherInstrumentationOptions]
           [graphql.schema DataFetcher]
           [graphql.schema.idl SchemaParser]
           [graphql.schema.idl RuntimeWiring]
           [graphql.schema.idl SchemaGenerator]
           [graphql.schema.idl TypeDefinitionRegistry]
           [org.dataloader BatchLoaderWithContext]
           [org.dataloader BatchLoaderContextProvider]
           [org.dataloader DataLoaderRegistry]
           [org.dataloader DataLoaderOptions]
           [org.dataloader DataLoaderFactory]))

(def ^:private gson (.. (GsonBuilder.)
                        setPrettyPrinting
                        create))

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

(deftype ^:private UnaryOperatorWrapper [f]
  java.util.function.UnaryOperator
  (apply [_ v]
    (f v)))

(deftype ^:private BatchLoaderWithContextWrapper [f]
  BatchLoaderWithContext
  (load [_ keys ctx]
    (f keys ctx)))

(defrecord ^:private Resolver
           [qualified-field
            object-type
            field
            option
            datafetcher
            batchloader])

(defn- make-batch-arg [{:keys [batch]} ctx args parent]
  {:ctx (select-keys ctx (:ctx batch))
   :args (select-keys args (:args batch))
   :parent (select-keys parent (:parent batch))})

(defn- ctx->m [ctx]
  (->> (.iterator (.stream ctx))
       iterator-seq
       (map (fn [eset]
              [(.getKey eset)
               (.getValue eset)]))
       (into {})))

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

(defn- jm->m [jm]
  (-> (j/from-java jm)
      keywordize-keys))

(deftype ^:private DataFetcherWrapper [qualified-field option resolve-fn]
  DataFetcher
  (get [_this env]
    (let [dataloader (.getDataLoader env (str qualified-field))
          ctx (or (ctx->m (.getContext env)) {})
          args (or (jm->m (.getArguments env)) {})
          parent (or (jm->m (.getSource env)) {})]
      (if (and dataloader (:batch option))
        (.load dataloader (make-batch-arg option ctx args parent))
        (stringify-keys (resolve-fn ctx args parent))))))

(defmulti resolver
  {:arglists '([& more])}
  (fn [name & _more] name))

(defmethod resolver :default [_] nil)

(defn drop-nth [n coll]
  (keep-indexed #(when (not= %1 n) %2) coll))

(defn- parse-fdecl
  [fdecl]
  (let [;; if first fdecl is string? then drop docstring
        fdecl                  (if (string? (second fdecl))
                                 (drop-nth 1 fdecl)
                                 fdecl)
        [qualified-field args] (if (keyword? (first fdecl))
                                 [(first fdecl) (rest fdecl)]
                                 [nil fdecl])
        [option [args & body]] (if (map? (first args))
                                 [(first args) (rest args)]
                                 [{} args])]
    {:qualified-field qualified-field
     :option          option
     :args            args
     :body            body}))

(defn- object-type->fields [object-type]
  (let [type (.getName object-type)
        fields (->> (.getFieldDefinitions object-type)
                    (map #(.getName %)))]
    (map (fn [field]
           {:object-type type
            :field field
            :qualified-field (keyword (str type "/" field))})
         fields)))

(defn- type-def-registry->object-types [type-def-registry]
  (.. type-def-registry
      types
      values
      iterator))

(defn- type-def-registry->fields [type-def-registry]
  (->> (type-def-registry->object-types type-def-registry)
       iterator-seq
       (map object-type->fields)
       (apply concat)))

(defn- datafetcher-register! [runtime-wiring-builder resolvers]
  (run!
   (fn [{:keys [object-type field datafetcher]}]
     (when (and object-type field datafetcher)
       (.. runtime-wiring-builder
           (type
            object-type
            (->UnaryOperatorWrapper
             (fn [builder]
               (.dataFetcher builder field datafetcher)))))))
   resolvers))


(defn- build-runtime-wiring [resolvers]
  (let [runtime-wiring-builder (RuntimeWiring/newRuntimeWiring)]
    (datafetcher-register!
     runtime-wiring-builder
     resolvers)
    (.build runtime-wiring-builder)))

(defn- build-resolvers [fields]
  (map (fn [{:keys [qualified-field]}]
         (resolver qualified-field)) fields))

(defn- build-schema [type-def-registry resolvers]
  (let [schema-generator (SchemaGenerator.)
        runtime-wiring (build-runtime-wiring resolvers)]
    (.. schema-generator
        (makeExecutableSchema
         type-def-registry
         runtime-wiring))))

(defn- build-dataloader-registry [resolvers loader-options]
  (let [registry (DataLoaderRegistry.)]
    (run! (fn [{:keys [qualified-field batchloader] :as _resolver}]
            (let [dataloader (DataLoaderFactory/newDataLoader
                              batchloader
                              loader-options)]
              (.register registry
                         (str qualified-field)
                         dataloader)))
          resolvers)
    registry))

(defmacro defresolver
  {:arglists '([qualified-field args body]
               [qualified-field docstring|options args body]
               [qualified-field docstring options args body])}
  ([& fdecl]
   (let [{:keys [qualified-field option args body]} (parse-fdecl fdecl)
         {:keys [batch]} option
         [_ object-type field] (re-matches
                                #":(\w+)/(\w+)"
                                (str qualified-field))]
     `(defmethod resolver
        ~qualified-field
        [_#]
        (let [resovler-fn# (fn ~args ~@body)
              datafetcher# (->DataFetcherWrapper
                            ~qualified-field
                            ~option
                            resovler-fn#)
              batchloader# (when
                            ~batch
                             (->BatchLoaderWithContextWrapper
                              (fn [keys# ctx#]
                                (p/create
                                 (fn [resolve# reject#]
                                   (try
                                     (let [results# (resovler-fn#
                                                     (.getContext ctx#)
                                                     keys#)]
                                       (-> results#
                                           stringify-keys
                                           resolve#))
                                     (catch Exception e#
                                       (reject# e#))))))))]
          (->Resolver ~qualified-field
                      ~object-type
                      ~field
                      ~option
                      datafetcher#
                      batchloader#))))))

(defn merge-type-def-registry [type-def-registries]
  (let [type-def-registry (TypeDefinitionRegistry.)]
    (run! (fn [tdr]
            (.merge type-def-registry tdr))
          type-def-registries)
    type-def-registry))

(defn build-prepared-schema [schema-files]
  (let [parser (SchemaParser.)
        merged-type-def-registry (->> schema-files
                                      (map (fn [sdl] (.parse parser sdl)))
                                      merge-type-def-registry)
        resolvers (-> merged-type-def-registry
                      type-def-registry->fields
                      build-resolvers)
        schema (build-schema merged-type-def-registry resolvers)]
    {:schema schema
     :resolvers resolvers}))

(defn execute-query [{:keys [schema resolvers]} query ctx]
  (let [context-provider (reify BatchLoaderContextProvider
                           (getContext [_this] ctx))
        dispatcher-options (.. (DataLoaderDispatcherInstrumentationOptions/newOptions)
                               (includeStatistics true))
        loader-options  (.. (DataLoaderOptions/newOptions)
                            (setBatchLoaderContextProvider context-provider))
        registry (build-dataloader-registry resolvers loader-options)
        dispatcher-instrumentation (DataLoaderDispatcherInstrumentation. dispatcher-options)
        executable (.. (GraphQL/newGraphQL schema)
                       (instrumentation dispatcher-instrumentation)
                       build)
        input (.. (ExecutionInput/newExecutionInput)
                  (query query)
                  (dataLoaderRegistry registry)
                  (graphQLContext ctx)
                  build)
        result (.execute executable input)]
    (.toJson gson result)))
