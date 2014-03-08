# ritual

__ritual__ offers database fixtures for Clojure tests.

[![Build Status](https://travis-ci.org/xsc/ritual.png)](https://travis-ci.org/xsc/ritual)
[![endorse](https://api.coderwall.com/xsc/endorsecount.png)](https://coderwall.com/xsc)

## Usage

__Leiningen__ ([via Clojars](https://clojars.org/ritual))

```clojure
[ritual "0.1.0-SNAPSHOT"] ;; unstable
```

__Clojure__

```clojure
(require '[ritual.core :refer :all])
```

### `table`

#### Basics

You can construct a function that creates a table and inserts a given set of rows using `ritual.core/table`:

```clojure
(def person-fixture
  (table
    [{:id 1 :name "Someone"}
     {:id 2 :name "Else"}
     ...]
    :primary-key :id))
```

By supplying a database spec and a table name, the changes are made. The result will be a
database spec that can be used for cleanup after all operations were performed. This enables
having multiple tracked tables by simply chaining the function calls:

```clojure
(def db
  (-> db-spec
      (person-fixture :person)
      (person-fixture :others)))
;; - creates table "person" (with columns "id" and "name"),
;; - inserts the given rows.
;; - creates table "others" (with columns "id" and "name"),
;; - inserts the given rows.

(cleanup db)
;; - drops the table "person"
;; - drops the table "others"
```

By default, columns types are inferred using the given data. Custom types/constraints can be supplied
using the `:overrides` key:

```clojure
(def non-nil-person-fixture
  (table
    [...]
    :primary-key :id
    :overrides {:id "integer"
                :name ["varchar(255)" "not null"]}))
```

#### Forcing a Clean Slate

By default, a table is only created if it does not exist. You can force dropping and recreation of a table
using the `:force?` option:

```clojure
(def db (person-fixture db-spec :person :force? true))
;; - drops table "person" if it exists,
;; - creates table "person" based on the given schema,
;; - inserts the given rows.
```

#### Prevent Inserting of Data

You can supply `:insert? false` to the creation function to prevent inserting of data:

```clojure
(def db (person-fixture db-spec :person :insert? false))
;; - creates table "person" if it doesn't exist,
;; - does not insert data.
```

#### Custom Cleanup

By default, a table is dropped once `ritual.core/cleanup` is called. You can supply a different cleanup strategy
using the `:cleanup` option:

- `:drop` (default): drops the table,
- `:clear`: removes all rows from the table,
- `:none`: don't touch the table,
- a vector of columns: the given columns' values will be collected using the initial data and only rows that match
  those values will be removed,
- a single column: same as above, albeit with only a single column.

For example, the following code will remove rows based on the `shop` column:

```clojure
(def products-fixture
  (table
    [{:id 1 :shop 22 :name "A"}
     {:id 2 :shop 22 :name "B"}
     {:id 3 :shop 22 :name "C"}]
    :primary-key :id))

(def db (products-fixture db-spec :products :cleanup [:shop]))
;; - creates table "products" if it doesn't exist,
;; - inserts the three rows.

(clojure.java.jdbc/insert! db :products [:id :shop :name] [4 23 "D"])
;; => [1]

(clojure.java.jdbc/query db "select count(*) as count from products")
;; => [{:count 4}]

(cleanup db)
;; - removes all rows with `shop = 22`,
;; - does not drop the table.

(clojure.java.jdbc/query db "select count(*) as count from products")
;; => [{:count 1}]
```

This is useful if you know that your DB logic will only affect a certain part of the data or if
you want to remove only what you inserted, enabling the non-destructive use of existing datasets.

You can attach explicit cleanup values to the database spec using `ritual.core/cleanup-include`:

```clojure
(def db
  (-> db-spec
      (products-fixture :products)
      (cleanup-include :products :shop [22 23])))
```

If you rerun the above example, no products should remain in the table.

### `snapshot` + `dump` + `diff`

The two functions `ritual.core/snapshot` and `ritual.core/dump` will read all values from a given
database table and create a map ordered by their primary key. `snapshot` will create a hash of each row,
while `dump` will return the actual data maps.

```clojure
(def db (person-fixture db-spec :people))

(snapshot db :people)
;; => {1 "c9fbf27657d4148108c50203e2507c7ccc835d40",
;;     2 "daa8839c65b695e1340b4477c85125fccac9bf88"}

(dump db :people)
;; => {1 {:name "Someone", :id 1},
;;     2 {:name "Else", :id 2}}
```

Snapshots can be used to decide _if_ changes occured and dumps can be used to analyze those changes.

```clojure
(clojure.java.jdbc/update! db :people {:name "Nobody"} ["id = ?" 1])
;; => [1]

(snapshot db :people)
;; => {1 "cd0e47e65a2277b29681ff641a71d59605141263",
;;     2 "daa8839c65b695e1340b4477c85125fccac9bf88"}

(dump db :people)
;; => {1 {:name "Nobody", :id 1},
;;     2 {:name "Else", :id 2}}
```

You can compare snapshots using `ritual.core/diff`. It will create a lazy seq of pairs of
`[:insert/:update/:delete <primary key>]`, e.g. for the above changes:

```clojure
(diff
  {1 "c9fbf27657d4148108c50203e2507c7ccc835d40", 2 "daa8839c65b695e1340b4477c85125fccac9bf88"}
  {1 "cd0e47e65a2277b29681ff641a71d59605141263", 2 "daa8839c65b695e1340b4477c85125fccac9bf88"})
;; => [[:update 1]]
```

Options to have a more targeted analysis of data are planned.

## License

Copyright &copy; 2014 Yannick Scherer

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
