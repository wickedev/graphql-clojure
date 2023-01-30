(ns core
  (:require [util :as util]
            [wrapper :as wrapper]
            [promesa.core :as p])
  (:import [graphql GraphQL]
           [graphql ExecutionInput]
           [graphql.execution.instrumentation.dataloader DataLoaderDispatcherInstrumentation]
           [graphql.execution.instrumentation.dataloader DataLoaderDispatcherInstrumentationOptions]
           [graphql.schema.idl SchemaParser]
           [graphql.schema.idl RuntimeWiring]
           [graphql.schema.idl SchemaGenerator]
           [graphql.schema.idl TypeDefinitionRegistry]
           [org.dataloader BatchLoaderContextProvider]
           [org.dataloader DataLoaderRegistry]
           [org.dataloader DataLoaderOptions]
           [org.dataloader DataLoaderFactory]))

(defmulti resolver
  {:arglists '([& more])}
  (fn [name & _more] name))

(defmethod resolver :default [_] nil)

(defn- parse-fdecl
  [fdecl]
  (let [;; if first fdecl is string? then drop docstring
        fdecl                  (if (string? (second fdecl))
                                 (util/drop-nth 1 fdecl)
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
              datafetcher# (wrapper/->DataFetcherWrapper
                            ~qualified-field
                            ~option
                            resovler-fn#)
              batchloader# (when
                            ~batch
                             (wrapper/->BatchLoaderWithContextWrapper
                              (fn [keys# ctx#]
                                (p/create
                                 (fn [resolve# reject#]
                                   (try
                                     (let [results# (resovler-fn#
                                                     (.getContext ctx#)
                                                     keys#)]
                                       (-> results#
                                           util/stringify-keys
                                           resolve#))
                                     (catch Exception e#
                                       (reject# e#))))))))]
          (wrapper/->Resolver ~qualified-field
                              ~object-type
                              ~field
                              ~option
                              datafetcher#
                              batchloader#))))))

(defn- object-type->resolvers [object-type]
  (let [type (.getName object-type)]
    (->> (.getFieldDefinitions object-type)
         (map #(.getName %))
         (map #(str type "/" %))
         (map (fn [qualified-field]
                [qualified-field (resolver (keyword qualified-field))]))
         (filter #(second %)))))

(defn- type-def-registry->object-types [type-def-registry]
  (.. type-def-registry
      types
      values))

(defn- build-resolvers [object-types]
  (into {} (->> object-types
                (map object-type->resolvers)
                (apply concat))))

(defn- build-schema [type-def-registry runtime-wiring]
  (let [schema-generator (SchemaGenerator.)]
    (.. schema-generator
        (makeExecutableSchema
         type-def-registry
         runtime-wiring))))

(defn- build-dataloader-registry [resolvers loader-options]
  (let [registry (DataLoaderRegistry.)]
    (run!
     (fn [{:keys [qualified-field batchloader] :as _resolver}]
       (let [dataloader (DataLoaderFactory/newDataLoader
                         batchloader
                         loader-options)]
         (.register registry
                    (str qualified-field)
                    dataloader)))
     resolvers)
    registry))

(defn merge-type-def-registry [type-def-registries]
  (let [type-def-registry (TypeDefinitionRegistry.)]
    (run! (fn [tdr]
            (.merge type-def-registry tdr))
          type-def-registries)
    type-def-registry))

(defn intersection-type-register!
  [runtime-wiring-builder object-type]
  (.type
   runtime-wiring-builder
   (.getName object-type)
   (wrapper/->UnaryOperatorWrapper
    (fn [builder]
      (.typeResolver
       builder
       (wrapper/->TypeResolverWrapper (fn [env] (prn env) env)))))))

(defn field-type-register!
  [runtime-wiring-builder resolvers type-name field]
  (let [field-name (str type-name "/" (.getName field))
        {:keys [datafetcher]} (get resolvers field-name)]
    (when datafetcher
      (.. runtime-wiring-builder
          (type
           type-name
           (wrapper/->UnaryOperatorWrapper
            (fn [builder]
              (.dataFetcher builder (.getName field) datafetcher))))))))

(defn object-type-register!
  [runtime-wiring-builder resolvers object-type]
  (let [type-name (.getName object-type)
        fields (.getFieldDefinitions object-type)
        field-type!' (partial field-type-register!
                              runtime-wiring-builder
                              resolvers
                              type-name)]

    (run! field-type!' fields)))

(defn build-runtime [object-types]
  (let [runtime-wiring-builder (RuntimeWiring/newRuntimeWiring)
        resolvers (build-resolvers object-types)
        intersection-type-register!' (partial
                                      intersection-type-register!
                                      runtime-wiring-builder)
        object-type-register!' (partial
                                object-type-register!
                                runtime-wiring-builder
                                resolvers)]
    (run! (fn [object-type]
            (condp = (type object-type)
              graphql.language.InterfaceTypeDefinition
              (intersection-type-register!' object-type)

              graphql.language.UnionTypeDefinition
              (intersection-type-register!' object-type)

              graphql.language.ObjectTypeDefinition
              (object-type-register!' object-type)))
          object-types)
    {:runtime-wiring (.build runtime-wiring-builder)
     :resolvers resolvers}))

(defn build-prepared-schema [schema-files]
  (let [parser (SchemaParser.)
        dispatcher-options (.. (DataLoaderDispatcherInstrumentationOptions/newOptions)
                               (includeStatistics true))
        dispatcher-instrumentation  (DataLoaderDispatcherInstrumentation. dispatcher-options)
        merged-type-def-registry (->> schema-files
                                      (map (fn [sdl] (.parse parser sdl)))
                                      merge-type-def-registry)
        object-types (type-def-registry->object-types merged-type-def-registry)
        {:keys [resolvers runtime-wiring]} (build-runtime object-types)
        schema (build-schema merged-type-def-registry runtime-wiring)
        executable (.. (GraphQL/newGraphQL schema)
                       (instrumentation dispatcher-instrumentation)
                       build)]
    {:schema schema
     :executable executable
     :resolvers resolvers}))

(defn execute-query [{:keys [executable resolvers]} query ctx]
  (let [context-provider (reify BatchLoaderContextProvider
                           (getContext [_this] ctx))
        loader-options  (.. (DataLoaderOptions/newOptions)
                            (setBatchLoaderContextProvider context-provider))
        registry (build-dataloader-registry (vals resolvers) loader-options)
        input (.. (ExecutionInput/newExecutionInput)
                  (query query)
                  (dataLoaderRegistry registry)
                  (graphQLContext ctx)
                  build)
        result (.execute executable input)]
    (.toJson util/gson result)))
