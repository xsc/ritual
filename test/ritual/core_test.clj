(ns ritual.core-test
  (:require [midje.sweet :refer :all]
            [ritual.core :refer :all]
            [clojure.java.jdbc :as db]))

(def db-spec
  {:subprotocol "derby"
   :subname "memory:core-test"
   :create true})

(def people
  (table
    [{:id 1234 :name "Me"}
     {:id 5678 :name "You" :address "Here"}]
    :primary-key :id))

(let [db (atom db-spec)]
  (fact "about the initial database state."
        (db/query @db "select count(*) as count from people") => (throws Exception #"does not exist"))

  (fact "about creating a table."
        (swap! db people :people :insert? false) => @db
        (db/query @db "select count(*) as count from people") => [{:count 0}]
        (swap! db cleanup) => @db
        (db/query @db "select count(*) as count from people") => (throws Exception #"does not exist"))

  (fact "about creating/inserting data."
        (swap! db people :people) => @db
        (db/query @db "select count(*) as count from people") => [{:count 2}]
        (swap! db cleanup) => @db
        (db/query @db "select count(*) as count from people") => (throws Exception #"does not exist"))

  (fact "about creating/inserting data into multiple tables."
        (swap! db #(-> % (people :people) (people :other-people))) => @db
        (db/query @db "select count(*) as count from people") => [{:count 2}]
        (db/query @db "select count(*) as count from other_people") => [{:count 2}]
        (swap! db cleanup) => @db
        (db/query @db "select count(*) as count from people") => (throws Exception #"does not exist")
        (db/query @db "select count(*) as count from other_people") => (throws Exception #"does not exist"))

  (with-state-changes [(before :facts (swap! db people :people))
                       (after :facts (swap! db cleanup))]
    (fact "about snapshot creation."
          (keys (snapshot @db :people)) => (contains #{1234 5678})
          (keys (snapshot @db :people [:name])) => (contains #{1234 5678})
          (keys (snapshot @db :people [:name] :name)) => (contains #{"Me" "You"})
          (snapshot @db :people [:name :address]) => (snapshot @db :people [:address :name]))
    (fact "about snapshots + UPDATE"
          (let [s0 (snapshot @db :people)]
            (db/execute! @db ["update people set name='me?' where id=1234"]) => [1]
            (let [s1 (snapshot @db :people)]
              (s0 1234) =not=> (s1 1234)
              (s0 5678) => (s1 5678)
              (diff s0 s1) => [[:update 1234]])))
    (fact "about snapshots + INSERT"
          (let [s0 (snapshot @db :people)]
            (db/execute! @db ["insert into people (id,name) values (90, 'Him')"]) => [1]
            (let [s1 (snapshot @db :people)]
              (keys s0) =not=> (contains 90)
              (keys s1) => (contains 90)
              (dissoc s1 90) => s0
              (diff s0 s1) => [[:insert 90]])))
    (fact "about snapshots + DELETE"
          (let [s0 (snapshot @db :people)]
            (db/execute! @db ["delete from people where id=1234"]) => [1]
            (let [s1 (snapshot @db :people)]
              (s0 1234) =not=> (s1 1234)
              (s1 1234) => falsey
              (s0 5678) => (s1 5678)
              (diff s0 s1) => [[:delete 1234]])))))
