(ns postgres2datomic.core
  (:require [clojure.java.jdbc :as jdbc]
            [datomic.api :as d]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]])
  (:gen-class))


; Begin https://github.com/Datomic/day-of-datomic/blob/master/src/datomic/samples/query.clj helper functions 
(defn only
  "Return the only item from a query result"
  [query-result]
  (assert (= 1 (count query-result)))
  (assert (= 1 (count (first query-result))))
  (ffirst query-result))

  
(defn qe
  "Returns the single entity returned by a query."
  [query db & args]
  (let [res (apply d/q query db args)]
    (d/entity db (only res))))
  
; end https://github.com/Datomic/day-of-datomic/blob/master/src/datomic/samples/query.clj helper functions 

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
    
(defn get-mock-pg-rows [limit]
  "Mock data only works for mdl_user now"
  (map 
    #(hash-map
      :country "",
       :maildisplay 2,
       :timemodified 0,
       :icq "",
       :yahoo "",
       :firstaccess 0,
       :idnumber_vsa nil,
       :auth "geniusapis",
       :screenreader 0,
       :autosubscribe 1,
       :currentlogin 0,
       :lastaccess 0,
       :lastlogin 0,
       :timezone "America/New_York",
       :aim "",
       :confirmed 1,
       :msn "",
       :username_vsa nil,
       :lastip "",
       :secret "",
       :htmleditor 1,
       :city "",
       :username "user1",
       :skype "",
       :policyagreed 1,
       :imagealt nil,
       :mailformat 1,
       :idnumber_int 66244,
       :lang "en_utf8",
       :phone1 "",
       :url "",
       :email "user1@mailinator.com",
       :firstname (str "user-" %),
       :maildigest 0,
       :phone2 "",
       :emailstop 0,
       :mnethostid 1,
       :department "",
       :institution "",
       :trustbitmask 0,
       :lastname "test",
       :ajax 1,
       :theme "",
       :address "",
       :id 100387,
       :password "da6acd4d31d1823cb1583b5071520483",
       :description nil,
       :trackforums 0,
       :deleted 0,
       :picture 0,
      :idnumber (str %))
    (range 1 (+ 1 limit))))
    
(defn datomize-pg-table-col [table upsert_column_name {:keys[column_name data_type]}]
  "Convert a postgres table column to a datom"
  (merge
    {:db/id (d/tempid :db.part/db)
     :db/ident (keyword (str table "/" column_name))
     :db/valueType (keyword (str "db.type/" (type-map data_type)))
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}
    (when (= upsert_column_name column_name)
      {:db/unique :db.unique/identity})))

(defn datomize-pg-table-row [table row]
  "Convert a postgres table row to a datom"
  (conj
    {:db/id (d/tempid :db.part/user)}
    (into {} (for [[k v] row  :when (not-nil? v)] 
                  [(keyword (str table "/" (name k))) v]))))


(defn entity-attribute-history [e aname]
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
   (edn/read-string (slurp "config.edn")) 
  )
(defn get-pg-spec [& {:keys [config]
                      :or {config (get-config)}}]
  (:postgres config))
  
(defn get-pg-schema-tx-data [table & {:keys [upsert-column-name 
                                             pg-spec
                                             edn-output-file] 
                                              :or {pg-spec (get-pg-spec)}}]
  (let [datomize-pg-col   (partial datomize-pg-table-col table upsert-column-name)
        pg-table-cols     (get-pg-table-cols pg-spec table)
        schema-tx-data    (conj (map datomize-pg-col pg-table-cols)
                                {:db/id (d/tempid :db.part/db)
                                 :db/ident :pg/timecreated
                                 :db/valueType :db.type/instant
                                 :db/cardinality :db.cardinality/one
                                 :db.install/_attribute :db.part/db})]
          (when-not nil edn-output-file
            (spit edn-output-file (pr-str schema-tx-data)))
          schema-tx-data))
                  
(defn main
  "Main - Return db with schema loaded - Can be run from lein repl as shown below"
  ;Postgres2datomic.core=>  (do (require (ns-name *ns*) :reload-all)(main "mdl_sis_user_hist" :upsert_column_name "sis_user_idstr"))
  [table & {:keys [limit 
                   upsert_column_name 
                   edn-output-file] 
            :or { limit 100000
                  edn-output-file "schema.edn"
                }}]
  (let [config            (edn/read-string (slurp "config.edn")) 
        pg-spec           (:postgres config)
        datomic-uri       (get-in config [:datomic :uri])
        datomic-conn      (reset-datomic datomic-uri)
        datomize-pg-row   (partial datomize-pg-table-row table)
        schema-tx-data    (get-pg-schema-tx-data 
                            table 
                            :upsert-column-name upsert_column_name 
                            :pg-spec pg-spec
                            :edn-output-file edn-output-file) 
        pg-table-rows     (get-pg-table-rows pg-spec table limit)
        ;mock-rows         (get-mock-pg-rows 60000)
        rows              pg-table-rows 
        ;_                 (pprint (take 2 rows))
        data-tx-data      (map datomize-pg-row rows)
        ;_                 (pprint (take 2 data-tx-data))
        schema-tx-future  (d/transact datomic-conn schema-tx-data)
        _                 (pprint schema-tx-future)
        ;_                 (pprint data-tx-data)    
        transact          (partial d/transact datomic-conn)]
        (doseq [datom data-tx-data]
                  (pprint 
                    (transact 
                      [ datom, 
                        {:db/id (d/tempid :db.part/tx)
                        :pg/timecreated (->> datom
                                          ((keyword (str table "/timecreated")))
                                          (* 1000)
                                          (java.sql.Timestamp. ))}])))
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
           (def tx-instants (reverse (sort (d/q '[:find ?when :where [_ :db/txInstant ?when]]
                                                  db))))
            "a user"
            (def a-user (qe '[:find ?e :where [?e :mdl_sis_user_hist/username "icohen"]] db))
            (str "history of " (:mdl_sis_user_hist/username a-user) "'s passwords")
            (entity-attribute-history (:db/id a-user) :mdl_sis_user_hist/password)
            ]))))
   
      



                


