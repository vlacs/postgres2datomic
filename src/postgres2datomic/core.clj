(ns postgres2datomic.core
  (:require [clojure.java.jdbc :as jdbc]
            [datomic.api :as d :refer [db q]]
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

(defn typer [pg-type]
  "Convert a postgres column type to a datomic datum type"
  (cond
   (= pg-type "character varying") "string"
   (= pg-type "smallint") "long"
   (= pg-type "bigint") "long"
   (= pg-type "text") "string"))
   
(defn postgres-table-schema [db table]
  "Query a postgres database for a table schema"
  (jdbc/query db
    ["select table_name, column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = ?" table]))

(defn datumize [{:keys[table_name column_name data_type]}]
  "Convert a postgres table column type to a datum schema"
   {:db/id (d/tempid :db.part/db)
    :db/ident (keyword (str table_name "/" column_name))
    :db/valueType (keyword (str "db.type/" (typer data_type)))
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db})

(defn main
  "Main - used for testing for now..."
  [table]
  (let [config          (edn/read-string (slurp "config.edn")) 
        postgres-spec   (:postgres config)
        datomic-uri     (get-in config [:datomic :uri])
        datomic-conn    (reset-datomic datomic-uri)
        schema-tx       (map datumize (postgres-table-schema postgres-spec table))]
    @(d/transact datomic-conn schema-tx)))
  

