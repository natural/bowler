(ns natural.bowler.core
  (:require
   [clojure.spec.alpha :as s]
   [compojure.api.sweet :as cs]
   
   [spec-tools.core          :refer [create-spec]]
   [spec-tools.data-spec     :as ds]
   
   
   [natural.bowler.api :as api]
   [natural.bowler.db :as db]
   [natural.bowler.spec :refer [coercion]]))



(defn collection-as-response [{:keys [start stop rows count]} context]
  (let [c (second count)
        cr (str "items " start "-" (min (or c 0) (or stop 0)) "/" c)]
    (-> ((:as-response api/resource-config) rows context)
        (assoc :status 206)
        (assoc-in [:headers] {"accept-ranges" "items"})
        (assoc-in [:headers] {"content-range" cr}))))





(defn op-id [rel op-key]
  (keyword (name rel) (name op-key)))


(defn summary [k v & [suffix _]]
  (str (name k) " " v (or suffix "")))


(defn split-range [a]
  (let [[start stop] (map read-string (drop 1 (re-find (re-matcher #"^items=(\d+)\-(\d+)$" a))))]
    {:start start :stop stop}))

(defn partition-qualified-key [a]
  (-> a :partition :key))


(defn partition-key [a]
  (->> a
       partition-qualified-key
       name
       keyword))

(defn partition-exists? [config context]
  (db/partition-of config (-> context :request :path-params ((partition-key config)))))





(defn partition-qualified-name [a]
  (->> a
       partition-qualified-key
       (#(str (namespace %) "/" (name %)))))







(defn make-desc [n]
  {:create (str "Successful operation, new "          n " record created and returned in response body.")
   :delete (str "Successful delete, existing "        n " record removed.  Empty response.")
   :list   (str "Successful operation, zero or more " n " records in response body.")
   :read   (str "Successful read, existing "          n " record returned in response.")
   :update (str "Successful update, modified "        n " record returned in response body.")})



(defn resource-collection [config rel create-req create-resp many-resp]
  (let [part-key  (partition-key config)
        part-spec clojure.core/pos-int? ;; boo!
        rel-name  (name rel)
        desc      (make-desc rel-name)
        get-resp  (api/responses {206 {:schema many-resp :description (:list desc)}})
        put-resp  (api/responses {201 {:schema create-resp :description (:create desc)}})

        fk-cols   (into [] (filter (fn [{k :fkcolumn_name}] (not= k (name part-key)))
                                   (db/fks rel)))
        get-range #(or (-> % :request :header-params :range) "items=0-9")
        q-param   (merge
                   {(ds/opt :q) (create-spec {:spec string? :name "query"})}
                   (into {}
                         (map (fn [{t :pktable_name n :fkcolumn_name :as col}]
                                (let [ ;; why no (keyword rel-name n)
                                      ;; i.e., :table/column?
                                      ;; temp, justa this works for now
                                      sp pos-int?
                                      
                                      k (keyword n)]
                                  [(ds/opt k)
                                   (create-spec {:spec sp :name k})]))
                              fk-cols)))

        exists?   (fn [context]
                    (let [{part-val part-key} (partition-exists? config context)

                          range-header (get-range context)
                          list-q       (merge (split-range range-header)
                                              {:rel rel 
                                               :co-filters
                                               (merge {part-key part-val}
                                                      (filter
                                                       (fn [[a _]] (not (string? a)))
                                                       (merge (-> context :request :path-params)
                                                              (-> context :request :query-params))))                                               })]

                      (if part-val
                        {rel (db/list-resource config list-q)})))

        create!   (fn [context]
                    (let [b (-> context :request :body-params)
                          p (-> context :request :path-params)
                          [record _] (db/create-resource config
                                                         {:rel rel
                                                          :data (merge p b)})]
                      (if record
                        {rel record})))

        handler   {:tags [rel]
                   :put  {:summary     (summary :create rel-name)
                          :operationId (op-id rel :create)
                          :responses   put-resp
                          :parameters  (merge
                                        {:path-params {part-key (create-spec {:spec part-spec})}}
                                        {:body-params create-req})
                          :handler     (api/resource-handler
                                        :allowed-methods [:put]
                                        :allowed? (partial partition-exists? config)
                                        :new? true
                                        :put! create!
                                        :handle-created rel)}
                   :get  {:summary     (summary :list rel-name "s")
                          :operationId (op-id rel :list)
                          :responses   get-resp
                          :parameters  (merge
                                        {:query-params  q-param}
                                        {:header-params (merge api/auth-headers api/list-headers)}
                                        {:path-params   {part-key (create-spec {:spec part-spec})}})
                          :handler     (api/resource-handler
                                        :as-response collection-as-response
                                        :allowed-methods [:get]
                                        :exists? exists?
                                        :handle-ok rel)}}]
    (cs/context (str "/:" (name part-key)) []
                (cs/context (str "/" rel-name) []
                            (cs/resource (merge api/content-types handler))))))



(defn resource-detail [config rel read-resp update-req]
  (let [rel-name   (name rel)
        desc       (make-desc rel-name)
        id-key     (or (first (db/pks-keys rel)) :unknown)
        id-name    (name id-key)
        part-key   (partition-key config)

        part-q     (partition-qualified-key config)
        part-spec  (part-q (into {} ((-> part-q namespace keyword)
                                     (into {} @db/*col-specs*))))

        part-spec  (create-spec {:spec        clojure.core/pos-int?
                                 :description "partition key"})
        
        get-resp   (api/responses {200 {:schema read-resp :description (:read desc)}})
        del-resp   (api/responses {204 {:schema any? :description (:delete desc)}})
        post-resp  (api/responses {204 {:schema read-resp :description (:update desc)}})
        params     (assoc-in {:path-params {part-key part-spec}}
                             [:path-params id-key]
                             part-spec)

        exists?    (fn [context]
                     (let [p (-> context :request :path-params)]
                       (db/resource-exists? config {:rel rel
                                                    :co-filters {part-key (part-key p)
                                                                 id-key (id-key p)}})))

        delete!    (fn [context]
                     (let [p (-> context :request :path-params)]
                       (db/delete-resource config {:rel rel
                                                   :co-filters {part-key (part-key p)
                                                                id-key (id-key p)}})))

        update!    (fn [context]
                     (let [p (-> context :request :path-params)
                           b (-> context :request :body-params)]
                       (db/update-resource config {:rel rel
                                                   :data (merge p b)
                                                   :co-filters {part-key (part-key p)
                                                                id-key (id-key p)}})
                       (if-let [record (exists? context)]
                         (rel record))))
        
        handler    {:tags   [rel]
                    :get    {:summary     (summary :read rel-name)
                             :operationId (op-id rel :read)
                             :responses   get-resp
                             :parameters  params
                             :handler     (api/resource-handler :allowed-methods [:get]
                                                                :exists? exists?
                                                                :handle-ok rel)}
                    :delete {:summary     (summary :delete rel-name)
                             :operationId (op-id rel :delete)
                             :responses   del-resp
                             :parameters  params
                             :handler     (api/resource-handler :allowed-methods [:delete]
                                                                :exists? exists?
                                                                :handle-no-content delete!)}
                    :post   {:summary     (summary :update rel-name)
                             :operationId (op-id rel :update)
                             :responses   post-resp
                             :parameters  (merge params {:body-params update-req})
                             :handler     (api/resource-handler :allowed-methods [:post]
                                                                :exists? exists?
                                                                :respond-with-entity? true
                                                                :handle-ok update!)}}]
    (cs/context (str "/:" (name part-key)) []
                (cs/context (str "/" rel-name "/:" id-name) []
                            (cs/resource (merge api/content-types handler))))))




(defn signal-context [config rk sk]
  (let [nm  (name sk)
        ok (fn [{{p :path-params} :request}]
             (let [pk (partition-key config)]
               {:rows (db/read-resource-plain config
                                              {:rel sk :co-filters {pk (-> p pk)}})}))]
    (cs/context (str "/" (name rk) "/" nm) []
                (cs/resource
                 (merge
                  api/content-types
                  {:tags [(str (name rk) "/signal")]
                   :get  {:summary     (str "read " nm " signal")
                          :operationId (str (name rk) "/" nm)
                          :parameters  {:path-params {:user_id int?}}
                          :responses   {200 {:schema `(s/coll-of ~sk) :description (str sk " signal endpoint")}} 
                          :handler     (api/resource-handler :allowed-methods [:get]
                                                             :as-response     collection-as-response
                                                             :handle-ok       ok)}})))))



(defn api-handler [config]
  (cs/api
   {:coercion coercion
    :swagger {:ui   (get-in config [:api :path])
              :spec (get-in config [:api :swagger-path])
              :data {:info {:title       (get-in config [:ui :site :title])
                            :version     (get-in config [:ui :site :version])
                            :description (get-in config [:ui :site :description])}}}}

   (let [signals (->> config
                      :resources
                      (map #(vector (:key %)
                                    (concat (->> % :signals :item)
                                            (->> % :signals :node)))))]
     (eval `(cs/context (str (-> ~config :api :path) "/" (partition-key ~config) "/signal") []
                        ~@(for [[rk ks] signals
                                k ks]
                            `(signal-context ~config ~rk ~k)))))

   (let [pairs (->> config :resources (map #(vector (:rel %) (:proto %))))]
     (eval `(cs/context (-> ~config :api :path) []
                        ~@(for [[a b] pairs :let [c (keyword (name a))]]
                            `(resource-collection
                              ~config
                              ~c
                              (create-spec {:spec ~b :name ~b})
                              (create-spec {:spec ~a :name ~a})
                              (create-spec {:spec (s/coll-of ~a)
                                            :name (str (namespace ~a) "/" (name ~a) "s") })))
                        ~@(for [[a b] pairs :let [c (keyword (name a))]]
                            `(resource-detail
                              ~config
                              ~c
                              (create-spec {:spec ~a :name ~a})
                              (create-spec {:spec ~b :name ~b}))))))))

