(defproject postgres2datomic "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.datomic/datomic-free "0.8.4143"]
                 [org.clojure/java.jdbc "0.3.0-alpha5"]
                 [postgresql/postgresql "9.1-901.jdbc4"]]
  ;:jvm-opts ["-Xmx1g"]
  :main postgres2datomic.core)
