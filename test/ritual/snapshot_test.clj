(ns ritual.snapshot-test
  (:require [midje.sweet :refer :all]
            [ritual.snapshot :refer :all]
            [ritual.spec :as spec]
            [ritual.db :refer :all]
            [clojure.java.jdbc :as jdbc]))

(fact "about the snapshot SELECT query"
      (snapshot-query :table [:a :b]) => ["select a,b from table"]
      (snapshot-query :a-table [:a-or-c :b]) => ["select a_or_c,b from a_table"]
      (snapshot-query :table []) => ["select * from table"])

(let [db-spec (create-derby :snapshot)]
  (with-state-changes [(before :facts (->> [[:id :integer "PRIMARY KEY"]
                                            [:value "varchar(255)"]
                                            [:text "varchar(255)"]]
                                           (apply jdbc/create-table-ddl :test)
                                           (vector)
                                           (jdbc/execute! db-spec)))
                       (after :facts (->> (jdbc/drop-table-ddl :test)
                                          (vector)
                                          (jdbc/execute! db-spec)))]
    (fact "about an empty snapshot/dump"
          (snapshot db-spec :test) => (throws AssertionError #"no valid primary key")
          (dump db-spec :test) => (throws AssertionError #"no valid primary key")
          (snapshot db-spec :test :by :id) => empty?
          (dump db-spec :test :by :id) => empty?)

    (fact "about snapshots/dumps"
          (jdbc/insert! db-spec :test {:id 1234 :value "abc" :text "def"}) => truthy
          (jdbc/insert! db-spec :test {:id 5678 :value "abc" :text "def"}) => truthy
          (let [s0 (snapshot db-spec :test :by :id)
                s1 (snapshot db-spec :test :by :id :only [:value :text])]
            s0 => map?
            s0 =not=> empty?
            (keys s0) => (contains #{1234 5678})
            (vals s0) => (has every? string?)
            (s0 1234) =not=> (s0 5678)
            s1 => map?
            s1 =not=> empty?
            (set (keys s1)) => (set (keys s0))
            (vals s1) => (has every? string?)
            (s1 1234) => (s1 5678)
            (snapshot db-spec :test :by :id :only [:text :value]) => s1)
          (let [d (dump db-spec :test :by :id)]
            d => map?
            d =not=> empty?
            (keys d) => (contains #{1234 5678})
            (vals d) => (has every? map?)
            (d 1234) => {:id 1234 :value "abc" :text "def"}
            (d 5678) => {:id 5678 :value "abc" :text "def"}))

    (fact "about snapshots/dumps (based on metadata)"
          (jdbc/insert! db-spec :test {:id 1234 :value "abc" :text "def"}) => truthy
          (jdbc/insert! db-spec :test {:id 5678 :value "abc" :text "def"}) => truthy
          (let [db-spec' (-> db-spec
                             (spec/set-columns :test [:value :text])
                             (spec/set-primary-key :test :id))]
            db-spec' => db-spec
            (snapshot db-spec' :test) => (snapshot db-spec :test :by :id)
            (dump db-spec' :test) => (dump db-spec :test :by :id)))))
