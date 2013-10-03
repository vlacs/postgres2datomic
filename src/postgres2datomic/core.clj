(ns postgres2datomic.core
  (:require [clojure.java.jdbc :as jdbc]
            [datomic.api :as d]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]])
  (:gen-class))

(def not-nil? (complement nil?))
  
(def type-map {"character varying" "string"
               "smallint"          "long"
               "bigint"            "long"
               "text"              "string"})
(defn reset-datomic [uri]
  "Return a connection to a new database."
  ;; This will fail if the DB doesn't exist. But who cares?
  ;; See http://docs.datomic.com/clojure-api.html
  (d/delete-database uri)
  (d/create-database uri)
  (d/connect uri))

(defn keywordize [s]
  "Convert a string to an idomatic keyword"
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "." "-")
      (keyword)))

(defn get-pg-table-cols [db table]
  "Query a postgres database for table columns and data types"
  (jdbc/query db
    ["select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = ?" table]))

(defn get-pg-table-rows [db table]
  "Query a postgres database table for 100 rows...for now"
  (jdbc/query db
    [(str "select * from " table " limit 100000")]))

(defn datomize-pg-table-col [table {:keys[column_name data_type]}]
  "Convert a postgres table column to a datom"
   {:db/id (d/tempid :db.part/db)
    :db/ident (keyword (str table "/" column_name))
    :db/valueType (keyword (str "db.type/" (type-map data_type)))
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db})

(defn datomize-pg-table-row [table row]
  "Convert a postgres table row to a datom"
  (conj
    {:db/id (d/tempid :db.part/db)}
    (into {} (for [[k v] row  :when (not-nil? v)] 
                  [(keyword (str "mdl_user" "/" (name k))) v]))))

(defn main
  "Main - Return db with schema loaded"
  [table]
  (let [config            (edn/read-string (slurp "config.edn")) 
        pg-spec           (:postgres config)
        datomic-uri       (get-in config [:datomic :uri])
        datomic-conn      (reset-datomic datomic-uri)
        datomize-pg-col   (partial datomize-pg-table-col table)
        datomize-pg-row   (partial datomize-pg-table-row table)
        schema-tx-data    (map datomize-pg-col (get-pg-table-cols pg-spec table))
        ;_                 (pprint (take 1 schema-tx-data))
        data-tx-data      (map datomize-pg-row (get-pg-table-rows pg-spec table))

        schema-tx-future  @(d/transact datomic-conn schema-tx-data)
        ;_                 (pprint (take 2 data-tx-data))
        data-tx-future    @(d/transact datomic-conn data-tx-data)
        ]
        (def db (d/db datomic-conn))
        (dorun 
          (map pprint [
            "count all records with firstname"
            (d/q '[:find (count ?e)
                   :where [?e :mdl_user/firstname]]
                 db)
            "get all records with first name jared"     
            (d/q '[:find ?e
                   :in $ ?firstname
                   :where [?e :mdl_user/firstname ?firstname]]
                 db
                 "jared")
                 ]))))


