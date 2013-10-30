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
               "text"              "string"
               })
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

(defn get-pg-table-rows
  "Query a postgres database table"
  [db table limit]
  (jdbc/query db
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

(defn get-config []
   (edn/read-string (slurp "config.edn")))

(defn get-pg-spec [& {:keys [config]
                      :or {config (get-config)}}]
  (:postgres config))

(defn get-datomic-conn [& {:keys [config]
                           :or {config (get-config)}}]
  (let [datomic-uri       (get-in config [:datomic :uri])
        datomic-conn      (reset-datomic datomic-uri)]
    datomic-conn))

(defn get-pg-schema-tx-data [table & {:keys [upsert-column-name
                                             pg-spec
                                             edn-output-file]
                                      :or {pg-spec (get-pg-spec)}}]
  (let [datomize-pg-col   (partial datomize-pg-table-col table upsert-column-name)
        pg-table-cols     (get-pg-table-cols pg-spec table)
        timecreated-datom {:db/id (d/tempid :db.part/db)
                                 :db/ident :pg/timecreated
                                 :db/valueType :db.type/instant
                                 :db/cardinality :db.cardinality/one
                                 :db.install/_attribute :db.part/db}
        schema-tx-data    (conj (map datomize-pg-col pg-table-cols)
                                timecreated-datom)]
    (when-not nil edn-output-file
      (spit edn-output-file (pr-str schema-tx-data)))
    schema-tx-data))

(defn get-pg-rows-tx-data [table limit & {:keys [pg-spec
                                                 edn-output-file]
                                          :or {pg-spec (get-pg-spec)}}]
  (let [datomize-pg-row   (partial datomize-pg-table-row table)
        rows              (get-pg-table-rows pg-spec table limit)
        rows-tx-data      (map datomize-pg-row rows)]
    (when-not nil edn-output-file
      (spit edn-output-file (pr-str rows-tx-data)))
    rows-tx-data))


(defn table-to-edn [table & {:keys [pg-spec
                                    limit
                                    upsert-column-name
                                    schema-edn-output-file
                                    rows-edn-output-file]
                              :or {limit 100000}}]
  (let [schema-tx-data    (get-pg-schema-tx-data
                            table
                            :pg-spec            pg-spec
                            :edn-output-file    schema-edn-output-file
                            :upsert-column-name upsert-column-name)
        rows-tx-data      (get-pg-rows-tx-data
                            table
                            limit
                            :pg-spec            pg-spec
                            :edn-output-file    rows-edn-output-file)]
    {:table table :schema schema-tx-data :rows rows-tx-data}))


(defn import-schema [table {:keys [datomic-conn
                                   schema-tx-data]}]
  (d/transact datomic-conn schema-tx-data))

(defn import-rows [table {:keys [datomic-conn
                                 rows-tx-data]}]
  (doseq [datoms rows-tx-data]
    ((partial d/transact datomic-conn)
      datoms)))

(defn import-table [table & {:keys [pg-spec
                                    datomic-conn
                                    limit
                                    upsert-column-name
                                    schema-edn-output-file
                                    rows-edn-output-file]
                              :or {limit                  100000
                                   schema-edn-output-file "schema.edn"
                                   rows-edn-output-file   "rows.edn"}}]
  (let [{schema-tx-data   :schema
         rows-tx-data     :rows}
        (table-to-edn
          table
          :limit                  limit
          :pg-spec                pg-spec
          :schema-edn-output-file schema-edn-output-file
          :rows-edn-output-file   rows-edn-output-file
          :upsert-column-name     upsert-column-name)]
    (import-schema
      table
      {:datomic-conn   datomic-conn
       :schema-tx-data schema-tx-data})

    (import-rows
      table
      {:datomic-conn   datomic-conn
       :rows-tx-data   rows-tx-data})))

(defn main
  "Main - Return db with schema loaded - Can be run from lein repl as shown below"
  ;Postgres2datomic.core=>  (do (require (ns-name *ns*) :reload-all)(main "mdl_sis_user_hist" :upsert-column-name "sis_user_idstr"))
  [table & {:keys [limit
                   upsert-column-name
                   schema-edn-output-file
                   rows-edn-output-file]
            :or {limit 100000
                schema-edn-output-file "schema.edn"
                rows-edn-output-file   "rows.edn"}}]
  (let [config            (edn/read-string (slurp "config.edn"))
        pg-spec           (:postgres config)
        datomic-uri       (get-in config [:datomic :uri])
        datomic-conn      (reset-datomic datomic-uri)]
    (import-table
      table
      :pg-spec                  pg-spec
      :datomic-conn             datomic-conn
      :limit                    limit
      :upsert-column-name       upsert-column-name
      :schema-edn-output-file   schema-edn-output-file
      :rows-edn-output-file     rows-edn-output-file)

    (def db (d/db datomic-conn))
    (def conn datomic-conn)
    (def rules
      '[[[attr-in-namespace ?e ?ns2]
         [?e :db/ident ?a]
         [?e :db/valueType]
         [(namespace ?a) ?ns1]
         [(= ?ns1 ?ns2)]]])
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

