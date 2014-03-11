(ns ritual.table
  (:require [ritual
             [types :refer [*type-mapping*]]
             [utils :refer [sqlize]]]
            [clojure.java.jdbc :as jdbc]))

;; ## Check Existence

(defn exists?
  "Check whether a given table exists."
  [db-spec table-key]
  (try
    (do
      (->> (format "select 1 from %s" (sqlize table-key))
           (vector)
           (jdbc/query db-spec))
      true)
    (catch Throwable ex
      false)))

;; ## Create

(defn- create-column-vectors
  "Create column vectors based on the current type mapping and the given column
   types, primary key and a map of overrides (by column name)."
  [column-types primary-key override-map]
  (let [unknown-mapping (get *type-mapping* :unknown)
        primary-key-name (sqlize primary-key)
        overrides (->> (for [[k vs] override-map]
                         [(sqlize k) (if (coll? vs) vs [vs])])
                       (into {}))]
    (for [[column column-type] column-types
          :let [column-name (sqlize column)]]
      (vec
        (if-let [o (overrides column-name)]
          (cons column-name o)
          (let [type-name (get *type-mapping* column-type unknown-mapping)]
            (concat
              [column-name type-name]
              (when (= column-name primary-key-name)
                ["primary key"]))))))))

(defn create!
  "Create table using the given column types and primary key."
  [db-spec table-key column-types primary-key override-map]
  (->> (create-column-vectors column-types primary-key override-map)
       (apply jdbc/create-table-ddl (sqlize table-key))
       (vector)
       (jdbc/execute! db-spec))
  db-spec)

(defn create-if-not-exists!
  "Create table if it does not already exist."
  [db-spec table-key column-types primary-key override-map]
  (when-not (exists? db-spec table-key)
    (create! db-spec table-key column-types primary-key override-map))
  db-spec)

;; ## Drop

(defn drop!
  "Drop a given table."
  [db-spec table-key]
  (->> (jdbc/drop-table-ddl (sqlize table-key))
       (vector)
       (jdbc/execute! db-spec))
  db-spec)

(defn drop-if-exists!
  "Drop table if it exists."
  [db-spec table-key]
  (when (exists? db-spec table-key)
    (drop! db-spec table-key))
  db-spec)

;; ## Insert

(defn- sqlize-row
  "Sqlize the keys of a row map."
  [row]
  (->> (for [[k v] row]
         [(sqlize k) v])
       (into {})))

(defn insert!
  "Insert the given data maps into the given table using the given column type map."
  [db-spec table-key columns data]
  (let [column-names (mapv sqlize columns)
        column-juxt #(mapv (fn [k] (get % k)) column-names)
        data-rows (->> (map sqlize-row data)
                       (mapv column-juxt))]
    (apply jdbc/insert! db-spec (sqlize table-key) column-names data-rows)))

;; ## Selective Cleanup

(defn clean!
  "Remove rows from the given table using the given conditions"
  [db-spec table-key conditions]
  (if (seq conditions)
    (let [conditions (->> (for [[k v] conditions]
                            (when-let [vs (seq (if (coll? v) v [v]))]
                              (vector
                                (sqlize k)
                                (clojure.string/join "," (repeat (count vs) "?"))
                                vs)))
                          (filter identity))]
      (if (seq conditions)
        (let [query (->> (for [[k s _] conditions]
                           (format "%s in (%s)" k s))
                         (clojure.string/join " and ")
                         (str "delete from " (sqlize table-key) " where "))]
          (->> (mapcat last conditions)
               (cons query)
               (vec)
               (jdbc/execute! db-spec)))
        [0]))
    (jdbc/execute! db-spec [(format "delete from %s" (sqlize table-key))])))
