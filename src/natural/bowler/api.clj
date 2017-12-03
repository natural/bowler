(ns natural.bowler.api
  (:import [liberator.representation RingResponse]
           [java.sql SQLException])
  (:require
   [liberator.core :as lb]
   [liberator.representation :refer [ring-response as-response]]
   [ring.util.http-status :refer [get-name get-description]]
   [spec-tools.data-spec :as ds]))


(def content-types
  {:consumes ["application/json" "application/transit+json"]
   :produces ["application/json" "application/transit+json"]})


(defn code->response [code]
  {code {:name (get-name code)
         :description (str (get-name code) ". " (get-description code))
         :schema any?}})


(def responses
  (partial merge (apply merge (map code->response [400 401 403 503]))))


(def auth-headers
  {(ds/opt :auth_token) (merge
                         (ds/spec :auth_token string?)
                         {:description "Authorization token, as string"})})


(def list-headers
  {(ds/opt :range) (merge (ds/spec :range string?)
                          {:description "Request item range"
                           :json-schema/default "items=0-9"})})

(def resource-config
  (merge
   {:available-media-types (:consumes content-types)
    :can-post-to-missing?  false
    :can-put-to-missing?   false
    :handle-no-content     nil
    :handle-not-found      nil
    :new?                  false
    :respond-with-entity?  false}
   
   {:as-response           (fn [data context]
                             (if (= (type data) RingResponse)
                               (:ring-response data)
                               {:status (:status context) :body data}))

    :authorized?           (fn [{:keys [request] :as context}]
                             {:auth {:example :token}})

    :handle-exception      (fn [exc]
                             (if (= (type (:exception exc)) SQLException)
                               (ring-response {:status 400
                                               :body {:error (str (:exception exc))}})
                               (ring-response {:status 503
                                               :body {:error (str (:exception exc))}})))}))

(def resource-handler
  (partial lb/resource resource-config))

