(defproject ritual "0.1.0-SNAPSHOT"
  :description "Database Fixtures for Clojure"
  :url "https://github.com/xsc/ritual"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [pandect "0.3.0"]]
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :profiles {:test {:dependencies [[org.apache.derby/derby "10.10.1.1"]
                                   [midje "1.6.2"
                                    :exclusions [joda-time org.codehaus.plexus/plexus-utils]]
                                   [joda-time "2.3"]
                                   [org.codehaus.plexus/plexus-utils "3.0.17"]]
                    :plugins [[lein-midje "3.1.3"]]
                    :exclusions [org.clojure/clojure]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}}
  :aliases {"repl" ["with-profile" "+test" "repl"]
            "test" ["with-profile" "+test" "midje"]
            "all"  ["with-profile" "+1.4:+1.5:+1.6"]}
  :pedantic? :abort)
