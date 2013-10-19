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
    [(str "select * from " table " limit 100")]))
    
(defn get-mock-pg-rows [limit]
  "Mock data only works for mdl_user now"
  (map 
    #(hash-map
      ; :country "",
      ; :maildisplay 2,
      ; :timemodified 0,
      ; :icq "",
      ; :yahoo "",
      ; :firstaccess 0,
      ; :idnumber_vsa nil,
      ; :auth "geniusapis",
      ; :screenreader 0,
      ; :autosubscribe 1,
      ; :currentlogin 0,
      ; :lastaccess 0,
      ; :lastlogin 0,
      ; :timezone "America/New_York",
      ; :aim "",
      ; :confirmed 1,
      ; :msn "",
      ; :username_vsa nil,
      ; :lastip "",
      ; :secret "",
      ; :htmleditor 1,
      ; :city "",
      ; :username "user1",
      ; :skype "",
      ; :policyagreed 1,
      ; :imagealt nil,
      ; :mailformat 1,
      ; :idnumber_int 66244,
      ; :lang "en_utf8",
      ; :phone1 "",
      ; :url "",
      ; :email "user1@mailinator.com",
      ; :firstname (str "user-" %),
      ; :maildigest 0,
      ; :phone2 "",
      ; :emailstop 0,
      ; :mnethostid 1,
      ; :department "",
      ; :institution "",
      ; :trustbitmask 0,
      ; :lastname "test",
      ; :ajax 1,
      ; :theme "",
      ; :address "",
      ; :id 100387,
      ; :password "da6acd4d31d1823cb1583b5071520483",
      ; :description nil,
      ; :trackforums 0,
      ; :deleted 0,
      ; :picture 0,
      :idnumber (str %))
    (range 1 (+ 1 limit))))

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
    {:db/id (d/tempid :db.part/user)}
    (into {} (for [[k v] row  :when (not-nil? v)] 
                  [(keyword (str table "/" (name k))) v]))))


  
(defn main
  "Main - Return db with schema loaded - Can be run from lein repl as shown below"
  ;Postgres2datomic.core=>  (do (require (ns-name *ns*) :reload-all)(main "mdl_user"))
  [table]
  (let [config            (edn/read-string (slurp "config.edn")) 
        pg-spec           (:postgres config)
        datomic-uri       (get-in config [:datomic :uri])
        datomic-conn      (reset-datomic datomic-uri)
        datomize-pg-col   (partial datomize-pg-table-col table)
        datomize-pg-row   (partial datomize-pg-table-row table)
        pg-table-cols     (get-pg-table-cols pg-spec table)
        schema-tx-data    (map datomize-pg-col pg-table-cols)
        ;_                 (pprint (take 1 schema-tx-data))
        ; pg-table-rows     (get-pg-table-rows pg-spec table)
        mock-rows         (get-mock-pg-rows 20)
        rows              mock-rows
        _                 (pprint (take 2 rows))
        data-tx-data      (map datomize-pg-row rows)
        _                 (pprint (take 2 data-tx-data))
        schema-tx-future  (d/transact datomic-conn schema-tx-data)
        ;_                 (pprint (take 2 data-tx-data))    
        transact          (partial d/transact datomic-conn)
        ]
        (doseq [datom data-tx-data]
          (pprint @(transact [datom])))
        (def db (d/db datomic-conn))
        (dorun 
          (map pprint [
            "count all records with idnumber"
            (d/q '[:find (count ?e)
                   :where [?e :mdl_user/idnumber]]
                 db)
            ; "get all records with first name user-1"     
            ; (d/q '[:find ?e
            ;        :in $ ?firstname
            ;        :where [?e :mdl_user/firstname ?firstname]]
            ;      db
            ;      "user-1")
                 ]))))


