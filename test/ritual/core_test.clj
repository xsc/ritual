(ns ritual.core-test
  (:require [midje.sweet :refer :all]
            [ritual.core :refer :all]
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

;; ## Tests

(let [db (atom (create-derby :cores))]
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
        (db/query @db "select * from people" :row-fn (juxt :id :name :address)) => (contains #{[1234 "Me" nil] [5678 "You" "Here"]})
        (cleanup! db) => truthy
        (query-count! db "people") => (throws Exception #"does not exist"))

  (fact "about creating/inserting data into multiple tables."
        (swap! db #(-> % (people :people) (people :other-people))) => truthy
        (query-count! db "people") => 2
        (query-count! db "other_people") => 2
        (cleanup! db) => truthy
        (query-count! db "people") => (throws Exception #"does not exist")
        (query-count! db "other_people") => (throws Exception #"does not exist"))

  (with-state-changes [(before :facts (people! db :people :insert? false))
                       (after :facts (cleanup! db))]
    (fact "about insert without creating/dropping a table"
          (let [db' (people! db :people :force? false)]
            db' => @db
            (query-count! db "people") => 2
            (people! db :people :force? false) => (throws Exception)
            (cleanup db') => db'
            (query-count! db "people") => 0))))
