(ns ritual.core-test
  (:require [midje.sweet :refer :all]
            [ritual.core :refer :all]
            [ritual.table :refer [drop-if-exists!]]
            [ritual.db :refer :all]
            [clojure.java.jdbc :as db]))

;; ## Fixture Helpers

(def people
  (table
    [{:id 1234 :name "Me"}
     {:id 5678 :name "You" :address "Here"}]
    :primary-key :id
    :cleanup-by [:id]))

(defn people!
  [db-atom & args]
  (swap! db-atom #(apply people % args)))

(defn cleanup!
  [db-atom]
  (swap! db-atom cleanup))

(defn query-count!
  [db-atom table]
  (:count (first (db/query @db-atom (str "select count(*) as count from " table)))))

(defn query-data!
  [db-atom table]
  (db/query @db-atom (str "select * from " table)
            :row-fn (juxt :id :name :address)))

;; ## Tests

(let [db (atom (create-derby :cores))]
  (drop-if-exists! @db :people)

  (fact "about the initial database state."
        (query-count! db "people") => (throws Exception #"does not exist"))

  (fact "about creating a table."
        (people! db :people :insert? false) => truthy
        (query-count! db "people") => 0
        (people! db :people :insert? false) => truthy
        (query-count! db "people") => 0
        (cleanup! db) => truthy
        (query-count! db "people") => (throws Exception #"does not exist"))

  (fact "about creating/inserting data."
        (people! db :people) => truthy
        (query-count! db "people") => 2
        (query-data! db "people") => (contains #{[1234 "Me" nil] [5678 "You" "Here"]})
        (cleanup! db) => truthy
        (query-count! db "people") => (throws Exception #"does not exist"))

  (fact "about creating/inserting data into multiple tables."
        (swap! db #(-> % (people :people) (people :other-people))) => truthy
        (query-count! db "people") => 2
        (query-count! db "other_people") => 2
        (query-data! db "people") => (query-data! db "other_people")
        (cleanup! db) => truthy
        (query-count! db "people") => (throws Exception #"does not exist")
        (query-count! db "other_people") => (throws Exception #"does not exist"))

  (with-state-changes [(before :facts (people! db :people :insert? false))
                       (after :facts (cleanup! db))]
    (fact "about insert without creating/dropping a table"
          (let [db' (people @db :people :force? false)]
            db' => @db
            (query-count! db "people") => 2
            (people @db :people :force? false) => (throws Exception #"duplicate key value")
            (query-count! db "people") => 2
            (cleanup db') => db'
            (query-count! db "people") => 0)))

  (with-state-changes [(before :facts (people! db :people :force? false))
                       (after :facts (cleanup! db))]
    (fact "about selective cleanup."
          (db/insert! @db :people [:id] [90]) => [1]
          (query-count! db "people") => 3
          (query-data! db "people") => (contains #{[1234 "Me" nil] [5678 "You" "Here"] [90 nil nil]})
          (cleanup! db) => truthy
          (query-count! db "people") => 1)
          (query-data! db "people") => (contains #{[90 nil nil]})

  (with-state-changes [(before :facts (people! db :people))
                       (after :facts (cleanup! db))]
    (fact "about snapshots/dump based on DB spec."
          (let [s (snapshot @db :people)]
            s => map?
            (count s) => 2
            (keys s) => (contains #{1234 5678}))
          (dump @db :people) => {1234 {:id 1234 :name "Me"  :address nil}
                                 5678 {:id 5678 :name "You" :address "Here"}}))))
