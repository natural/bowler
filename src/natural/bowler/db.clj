(ns natural.bowler.db
  (:require [clojure.java.jdbc :refer [execute! insert! query]]
            [clojure.set :refer [union]]
            [honeysql.core :as honeysql :refer [qualify]]
            [natural.ascot :as ascot]))

;; prob not going to keep this around.

(def ^:dynamic *col-specs*   (atom {}))
(def ^:dynamic *proto-specs* (atom {}))
(def ^:dynamic *table-specs* (atom {}))
(def ^:dynamic *tables*      (atom {}))


;; initialize specs by reading db metadata.  threading macro doesn't
;; work here.
;;
(defn init! [{db-spec :db-spec disallow :disallow}]
  (if-let [tables (ascot/tables db-spec)]
    (reset! *tables* tables))
  
  (if (seq @*tables*)
    (let [tables @*tables*
          protos (map (fn [[k {:keys [columns primary-keys] :as table}]]
                        (let [ffn (fn [{cn :column_name :as col}]
                                    (let [ck  (keyword cn)
                                          pks (map :column_name primary-keys)]
                                      (not (or (.contains pks cn)
                                               (some #{ck} disallow)))))]
                          [k (assoc table :columns (filter ffn columns))]))
                      ;; map the tables to tables without some columns
                      tables)]
      
      (reset! *col-specs* (ascot/column-specs tables))
      (reset! *table-specs* (ascot/table-specs tables))
      (reset! *proto-specs* (ascot/table-specs protos :ns :proto))
      
      (ascot/register (apply concat (map second @*col-specs*)))
      (ascot/register @*table-specs*)
      (ascot/register @*proto-specs*)
      )))


(defn table [k]
  (k @*tables*))


(defn pks [rel]
  (->> rel table :primary-keys))


(defn fks [rel]
  (->> rel table :foreign-keys))


(defn pks-keys [rel]
  (into #{} (->> rel
                 pks
                 (map :column_name)
                 (map keyword))))


(defn format-ansi [q]
  (honeysql/format q :quoting :ansi))


;; returns partition {key id} if it exists
;;
(defn partition-of [{db-spec :db-spec :as config} value]
  (let [k (get-in config [:partition :key])
        i (keyword (name k))
        t (qualify (keyword (namespace k)))
        q {:select [i] :from [t] :where [:= i value]}]
    (first (query db-spec (format-ansi q)))))


(defn as-where [m]
  (vec (cons :and (for [[k v] m] [:= k v]))))


;; returns a list of resources
;;
(defn list-resource [{db-spec :db-spec} {:as   props
                                         :keys [rel start stop co-filters] 
                                         :or   {start 0 stop 9}}]
  (let [id-key    (or (first (pks-keys rel)) :invoice_id)
        rel       (qualify rel)
        start     (or start 0)
        limit     (inc (- stop start))
        where     (as-where co-filters)

        q         {:select [:*]
                   :from [rel]
                   :order-by [id-key]
                   :offset start
                   :limit limit
                   :where where}

        qc        {:select [:%count.*]
                   :from [rel]
                   :where where}]
    {:start start
     :stop  stop
     :rows  (query db-spec (format-ansi q))
     :count (ffirst (query db-spec (format-ansi qc)))}))


;; returns a single resource
;;
(defn read-resource [{db-spec :db-spec} {:keys [rel co-filters]}]
  (first (query db-spec (format-ansi {:select [:*]
                                      :from   [(qualify rel)]
                                      :where  (as-where co-filters)}))))


;; creates a single resource
;;
(defn create-resource [{db-spec :db-spec} {:keys [rel data]}]
  (insert! db-spec (name rel) data))


;; modifies an existing resource
;;
(defn update-resource [{:keys [db-spec immutables partition]} {:keys [rel co-filters data]}]
  (let [exclude (union (set immutables) (set [(first partition)]))
        updates (apply dissoc data exclude)
        where   (as-where co-filters)]
    (execute! db-spec (honeysql/format {:update rel
                                        :set    updates
                                        :where  where}))))


;; deletes an existing resource
;;
(defn delete-resource [{db-spec :db-spec} {:keys [rel co-filters]}]
  (execute! db-spec (honeysql/format {:delete-from rel :where (as-where co-filters)})))


(defn resource-exists? [config {rel :rel :as desc}]
  (if-let [v (read-resource config desc)]
    {rel v}))



(defn read-resource-plain [{db-spec :db-spec} {:keys [rel co-filters]}]
  (query db-spec (format-ansi {:select [:*]
                               :from   [(qualify rel)]
                               :where  (as-where co-filters)
                               :limit 200})))





