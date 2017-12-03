(ns natural.bowler.spec
  (:require
   [clj-time.coerce :as tc]

   [compojure.api.coercion.spec]
   [spec-tools.conform :as conform]
   [spec-tools.core :as st :refer [create-spec]]
   [spec-tools.json-schema :refer [accept-spec]]))



(def coercion
  (-> compojure.api.coercion.spec/default-options
      (assoc-in [:body :formats "application/json"]
                (st/type-conforming (merge conform/json-type-conforming
                                           {:timestamp (fn [a b] (tc/to-sql-time b))
                                            :interval  (fn [a b] (str b))
                                            :other     (fn [a b] (str b))
                                            :date      (fn [a b] (tc/to-sql-date b))
                                            :bytea     (fn [a b] (str b)) ;; check when posting images
                                            :double    (fn [a b] (double b))}
                                           conform/strip-extra-keys-type-conforming)))

      (assoc-in [:response :formats "application/json"]
                (st/type-conforming (merge conform/json-type-conforming
                                           {:timestamp (fn [a b] (tc/to-string b))
                                            :interval  (fn [a b] (str b))
                                            :double    (fn [a b] (double b))
                                            :bytea     (fn [a b] (-> (java.util.Base64/getEncoder) (.encodeToString b)))
                                            :other     (fn [a b] (str b))
                                            :date      (fn [a b] (str b))}
                                           conform/strip-extra-keys-type-conforming)))
      compojure.api.coercion.spec/create-coercion))
