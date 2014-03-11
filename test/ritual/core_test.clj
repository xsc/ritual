(ns ritual.core-test
  (:require [midje.sweet :refer :all]
            [ritual.core :refer :all]
            [ritual.table :refer [drop-if-exists!]]
            [ritual.db :refer :all]
            [clojure.java.jdbc :as db]
            [pandect.core :refer [sha1]]))

;; ## Fixture Helpers

(defn query-count!
  [db-atom table]
  (:count (first (db/query @db-atom (str "select count(*) as count from " table)))))

(defn query-data!
  [db-atom table]
  (db/query @db-atom (str "select * from " table)
            :row-fn (juxt :id :name :address)))

;; ## Basic Tests

(def people
  (table
    [{:id 1234 :name "Me"}
     {:id 5678 :name "You" :address "Here"}]
    :primary-key :id))

(let [db* (create-derby :core)
      db (atom nil)]
  (with-state-changes [(before :facts (do
                                        (reset! db db*)
                                        (drop-if-exists! @db :people)
                                        (drop-if-exists! @db :other-people))) ]
    (fact "about the initial database state."
          (query-count! db "people") => (throws Exception #"does not exist"))

    (fact "about creating/dropping a table."
          (swap! db people "people" :insert? false) => truthy
          (query-count! db "people") => 0
          (swap! db people "people" :insert? false) => truthy
          (query-count! db "people") => 0
          (swap! db cleanup) => truthy
          (query-count! db "people") => (throws Exception #"does not exist"))

    (fact "about not dropping a table if it was not created using the given spec."
          (let [db' (people @db "people" :insert? false)]
            (query-count! db "people") => 0
            (swap! db people "people" :insert? false) => truthy
            (query-count! db "people") => 0
            (swap! db cleanup) => truthy
            (query-count! db "people") => 0
            (cleanup db') => truthy
            (query-count! db "people") => (throws Exception #"does not exist")))

    (fact "about removing only the inserted data if the table already existed."
          (let [db' (people @db "people" :insert? false)]
            (query-count! db "people") => 0
            (swap! db people "people") => truthy
             (db/insert! @db :people [:id] [90]) => [1]
            (query-count! db "people") => 3
            (swap! db cleanup) => truthy
            (query-count! db "people") => 1
            (query-data! db "people") => [[90 nil nil]]
            (cleanup db') => truthy
            (query-count! db "people") => (throws Exception #"does not exist")))

    (fact "about creating/inserting data."
          (swap! db people "people") => truthy
          (query-count! db "people") => 2
          (query-data! db "people") => (contains #{[1234 "Me" nil] [5678 "You" "Here"]})
          (swap! db cleanup) => truthy
          (query-count! db "people") => (throws Exception #"does not exist"))

    (fact "about creating/inserting data into multiple tables."
          (swap! db #(-> % (people :people) (people :other-people))) => truthy
          (query-count! db "people") => 2
          (query-count! db "other_people") => 2
          (query-data! db "people") => (query-data! db "other_people")
          (swap! db cleanup) => truthy
          (query-count! db "people") => (throws Exception #"does not exist")
          (query-count! db "other_people") => (throws Exception #"does not exist"))

    (fact "about creating a table only if it doesn't exist."
          (swap! db people "people") => truthy
          (swap! db people "people" :insert? false) => truthy
          (query-count! db "people") => 2
          (swap! db people "people") => (throws Exception #"duplicate key value"))

    (fact "about forcing the creation of a table (and thus deleting all existing data)."
          (swap! db people "people") => truthy
          (swap! db people "people" :insert? false :force? true) => truthy
          (query-count! db "people") => 0
          (swap! db people "people") => truthy
          (query-count! db "people") => 2)

    (facts "about cleanup via drop."
           (fact "default behaviour"
             (swap! db people "people") => truthy
             (swap! db cleanup) => truthy
             (query-count! db "people") => (throws Exception #"does not exist"))
           (fact "explicit `:drop`"
             (swap! db people "people" :cleanup :drop) => truthy
             (swap! db cleanup) => truthy
             (query-count! db "people") => (throws Exception #"does not exist")))

    (fact "about suppressing cleanup."
          (swap! db people "people" :cleanup :none) => truthy
          (swap! db cleanup) => truthy
          (query-count! db "people") => 2)

    (fact "about non-dropping cleanup."
          (swap! db people "people" :cleanup :clear) => truthy
          (swap! db cleanup) => truthy
          (query-count! db "people") => 0)

    (facts "about selective cleanup."
           (fact "about cleaning up data by primary key."
             (swap! db people "people" :cleanup :id) => truthy
             (db/insert! @db :people [:id] [90]) => [1]
             (query-count! db "people") => 3
             (swap! db cleanup) => truthy
             (query-count! db "people") => 1
             (query-data! db "people") => [[90 nil nil]])
           (fact "about cleaning up data by explicit column values."
             (swap! db people "people" :cleanup :id) => truthy
             (db/insert! @db :people [:id] [90]) => [1]
             (query-count! db "people") => 3
             (swap! db cleanup-include "people" :id [90])
             (swap! db cleanup) => truthy
             (query-count! db "people") => 0)
           (fact "about cleaning up data by some column."
             (swap! db people "people" :cleanup :name) => truthy
             (db/insert! @db :people [:id :name] [90 "Me"] [121 "You"] [234 "Him"]) => [1 1 1]
             (query-count! db "people") => 5
             (swap! db cleanup) => truthy
             (query-count! db "people") => 1
             (query-data! db "people") => [[234 "Him" nil]])))

  (tabular
    (fact "about snapshots based on DB spec."
          (drop-if-exists! @db :people)
          (swap! db people "people") => truthy
          (let [s (apply snapshot @db :people ?args)
                k (set ?keys)]
            s => map?
            (count s) => (count k)
            (keys s) => (contains k)
            s => (contains ?part)))
    ?args               ?keys            ?part
    []                  [1234 5678]      {}
    [:by :id]           [1234 5678]      {}
    [:by :name]         ["Me" "You"]     {}
    [:by :address]      ["Here"]         {}
    [:only [:id]]       [1234 5678]      {1234 (sha1 "1234") 5678 (sha1 "5678")}
    [:only [:name]]     [1234 5678]      {1234 (sha1 "\"Me\"") 5678 (sha1 "\"You\"")}
    [:only [:id :name]] [1234 5678]      {1234 (sha1 "1234,\"Me\"") 5678 (sha1 "5678,\"You\"")}
    [:only [:name :id]] [1234 5678]      {1234 (sha1 "1234,\"Me\"") 5678 (sha1 "5678,\"You\"")}))

;; ## Tests for Overrides

(def custom-people
  (table
    [{:id 1234 :name "Me"}
     {:id 5678 :name "You" :address "Here"}]
    :primary-key :id
    :overrides {:address ["varchar(255)" "not null"]}))

(let [db (atom (create-derby :core-custom))]
  (drop-if-exists! @db :people)
  (fact "about overriding table fields (without insert)."
        (swap! db custom-people :people :insert? false) => truthy
        (cleanup @db) => truthy)

  (fact "about overriding table fields with column constraints."
        (swap! db custom-people :people) => (throws Exception #"cannot accept a NULL value")
        (cleanup @db) => truthy))

;; ## Tests for auto-generated Keys

(def auto-people
  (table
    [{:name "Me"}
     {:name "You" :address "Here"}]
    :primary-key :id
    :auto-generate [:id]
    :overrides {:id ["integer" "primary key" "generated always as identity (start with 1, increment by 1)"]}))

(let [db (atom (create-derby :core-auto))]
  (drop-if-exists! @db :people)
  (fact "about auto-generated keys."
        (swap! db auto-people "people") => truthy
        (let [s (snapshot @db "people")]
          s => map?
          (count s) => 2
          (keys s) => (has every? integer?))
        (let [d (dump @db "people")]
          (vals d) => (has every? (comp integer? :id)))
        (swap! db cleanup) => truthy))
