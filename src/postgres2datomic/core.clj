(ns postgres2datomic.core
  (:require [clojure.java.jdbc :as jdbc]
            [datomic.api :as d]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]])
  (:gen-class))
  
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

(defn pg-to-datomic-data-type [pg-type]
  "Convert a postgres column type to a datomic datum type"
  (let [type-map (clojure.walk/keywordize-keys 
                  {"character varying" "string"
                   "smallint" "long"
                   "bigint" "long"
                   "text" "string"})]
  (type-map (keyword pg-type))))

(defn get-pg-table-cols [db table]
  "Query a postgres database for table columns and data types"
  (jdbc/query db
    ["select table_name, column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = ?" table]))

(defn get-pg-table-rows [db table]
  "Query a postgres database table for 100 rows...for now"
  (jdbc/query db
    [(str "select * from " table " limit 100")]))

(defn datomize-pg-col [{:keys[table_name column_name data_type]}]
  "Convert a postgres table column to a datom"
   {:db/id (d/tempid :db.part/db)
    :db/ident (keyword (str table_name "/" column_name))
    :db/valueType (keyword (str "db.type/" (pg-to-datomic-data-type data_type)))
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db})

(defn datomize-pg-row [row]
  "Convert a postgres table row to a datom"
  row)

(defn main
  "Main - Return db with schema loaded"
  [table]
  (let [config            (edn/read-string (slurp "config.edn")) 
        pg-spec           (:postgres config)
        datomic-uri       (get-in config [:datomic :uri])
        datomic-conn      (reset-datomic datomic-uri)
        schema-tx-data    (map datomize-pg-col (get-pg-table-cols pg-spec table))
        data-tx-data      (map datomize-pg-row (get-pg-table-rows pg-spec table))
        schema-tx-future  @(d/transact datomic-conn schema-tx-data)
        ;data-tx-future    @(d/transact datomic-conn data-tx-data)
        ]
        (def db (d/db datomic-conn))
        ;; find attributes in the table namespace
        (pprint data-tx-data)
        (d/q '[:find ?ident
              :in $ ?ns
               :where
               [?e :db/ident ?ident]
               [_ :db.install/attribute ?e]
               [(namespace ?ident) ?ns]]
             db
             table)))


