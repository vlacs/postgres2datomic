(ns postgres2datomic.core
  (:require [clojure.java.jdbc :as jdbc]
            [datomic.api :as d]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]))

; Some helper functions from  https://github.com/Datomic/day-of-datomic/blob/master/src/datomic/samples/query.clj

(defn only
  "Return the only item from a query result"
  [query-result]
  (pprint query-result)
  (assert (= 1 (count query-result)))
  (assert (= 1 (count (first query-result))))
  (ffirst query-result))


(defn qe
  "Returns the single entity returned by a query."
  [query db & args]
  (let [res (apply d/q query db args)]
    (d/entity db (only res))))


(def not-nil? (complement nil?))


(def type-map {"character varying" "string"
               "smallint"          "long"
               "bigint"            "long"
               "integer"           "long"
               "text"              "string"})


(defn get-config []
  (edn/read-string (slurp "config.edn")))


(defn get-pg-spec []
 (:postgres (get-config)))


(defn reset-datomic [uri]
 "Return a connection to a new database."
 ;; This will fail if the DB doesn't exist. But who cares?
 ;; See http://docs.datomic.com/clojure-api.html
 (d/delete-database uri)
 (d/create-database uri)
 (d/connect uri))


(defn get-datomic-conn []
 (let [datomic-uri       (get-in (get-config) [:datomic :uri])
       datomic-conn      (reset-datomic datomic-uri)]
   datomic-conn))


 (def default-args  {:limit                  100000
                     :pg-spec                (get-pg-spec)
                     :datomic-conn           (get-datomic-conn)})


(defn get-pg-table-cols [{:keys [pg-spec
                                 table]}]
  "Query a postgres database for table columns and data types"
  (jdbc/query pg-spec
    ["select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = ?" table]))


(defn get-pg-table-rows
  "Query a postgres database table"
  [{ :keys [pg-spec
            table
            limit]}]
  (jdbc/query pg-spec
    [(str "select * from " table " where username = 'icohen' or username = 'moquist' limit " limit)]))


(defn datomize-pg-table-col [table upsert-column-name {:keys[column_name data_type]}]
  "Convert a postgres table column to a datom"
  (merge
    {:db/id (d/tempid :db.part/db)
     :db/ident (keyword (str table "/" column_name))
     :db/valueType (keyword (str "db.type/" (type-map data_type)))
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}
    (when (= upsert-column-name column_name)
      {:db/unique :db.unique/identity})))


(defn datom-to-timecreated [table datom]
  (->> datom
    ((keyword (str table "/timecreated")))
    (* 1000)
    (java.sql.Timestamp. )))


(defn timecreated-to-tx-datom [ timecreated ]
  {:db/id (d/tempid :db.part/tx)
  :pg/timecreated timecreated})


(defn datomize-pg-table-row [table row]
  "Convert a postgres table row to a datom"
  (let
    [row-datom          (conj
                          {:db/id (d/tempid :db.part/user)}
                          (into {} (for [[k v] row  :when (not-nil? v)]
                            [(keyword (str table "/" (name k))) v])))
     timecreated-datom  (->>  row-datom
                              ((partial datom-to-timecreated table))
                              (timecreated-to-tx-datom))]
    [row-datom, timecreated-datom]))


(defn entity-attribute-history [db e aname]
  "Get history of entity attribute values"
  (->> (d/q '[:find ?aname ?v ?timecreated
              :in $ ?e ?aname
              :where
              [?e ?a ?v ?tx ?added]
              [?a :db/ident ?aname]
              [(= ?added true)]
              [?tx :pg/timecreated ?timecreated]]
            (d/history db)
            e aname)
       seq
       (sort-by #(nth % 2))))


(defn get-pg-schema-tx-data [{:keys [pg-spec
                                     table
                                     upsert-column-name]
                               :as fn-args}]
  (let [datomize-pg-col   (partial datomize-pg-table-col table upsert-column-name)
        pg-table-cols     (get-pg-table-cols fn-args)
        timecreated-datom {:db/id (d/tempid :db.part/db)
                                 :db/ident :pg/timecreated
                                 :db/valueType :db.type/instant
                                 :db/cardinality :db.cardinality/one
                                 :db.install/_attribute :db.part/db}
        schema-tx-data    (conj (map datomize-pg-col pg-table-cols)
                                timecreated-datom)]
    schema-tx-data))


(defn get-pg-rows-tx-data [{:keys [pg-spec
                                   table
                                   limit]
                            :as fn-args}]
  (let [datomize-pg-row   (partial datomize-pg-table-row table)
        rows              (get-pg-table-rows fn-args)
        rows-tx-data      (map datomize-pg-row rows)]
    rows-tx-data))


(defn table-to-edn [{:keys [pg-spec
                            table
                            limit
                            upsert-column-name]
                      :as fn-args}]
  (let [import-spec (merge default-args fn-args)
        schema-tx-data    (get-pg-schema-tx-data import-spec)
        rows-tx-data      (get-pg-rows-tx-data import-spec)]
    {:table             table
     :schema-tx-data    schema-tx-data
     :rows-tx-data      rows-tx-data}))


(defn import-schema! [{:keys [datomic-conn
                             schema-tx-data]}]
  (d/transact datomic-conn schema-tx-data))


(defn import-rows! [{:keys [datomic-conn
                          rows-tx-data]}]
  (doseq [datoms rows-tx-data]
    (d/transact datomic-conn datoms)))


(defn import-table! [{:keys [pg-spec
                            table
                            limit
                            datomic-conn]
                     :as fn-args}]
  (let [import-spec (merge 
                      default-args 
                      fn-args)
        table-spec (table-to-edn import-spec)
        param-spec (merge import-spec table-spec)]
    (import-schema! param-spec)
    (import-rows! param-spec)))


(defn query-table [{:keys [ table
                            datomic-conn]}]
  "Debugging queries to run after import-table"
  (let [db (d/db datomic-conn)
        rules '[[[attr-in-namespace ?e ?ns2]
                 [?e :db/ident ?a]
                 [?e :db/valueType]
                 [(namespace ?a) ?ns1]
                 [(= ?ns1 ?ns2)]]]]
     (dorun
       (map pprint [
        ; "list all attributes in the table namespace"
        ; (d/q '[:find ?e
        ;        :in $ ?t
        ;        :where
        ;        [?e :db/valueType]
        ;        [?e :db/ident ?a]
        ;        [(namespace ?a) ?ns]
        ;        [(= ?ns ?t)]]
        ;      db table)
         (str "count all entities possessing *any* " table " attribute")
         (d/q '[:find (count ?e)
                :in $ % ?t
                :where
                (attr-in-namespace ?a ?t)
                [?e ?a]]
              db rules table)
         "tx-instants"
         (reverse (sort (d/q '[:find ?when :where [_ :db/txInstant ?when]] db)))
         "timecreateds"
         (reverse (sort (d/q '[:find ?when :where [_ :pg/timecreated ?when]] db)))
         "a user"
         (def a-user (qe '[:find ?e :where [?e :mdl_sis_user_hist/username "icohen"]] db))
         (str "history of " (:mdl_sis_user_hist/username a-user) "'s passwords")
         (entity-attribute-history db (:db/id a-user) :mdl_sis_user_hist/password)]))))


(defn import-table-and-query!
  "Debugging fn. Probably delete this later...
   Returns import-spec"
  ;Postgres2datomic.core=>  (do (require (ns-name *ns*) :reload-all)(import-table-and-query "mdl_sis_user_hist" :upsert-column-name "sis_user_idstr"))
  [{:keys [table
           limit
           upsert-column-name
           schema-edn-output-file
           rows-edn-output-file]
     :as fn-args}]
  (let [import-spec  (merge default-args fn-args)]
    (import-table! import-spec)
    (query-table import-spec)
    import-spec))
