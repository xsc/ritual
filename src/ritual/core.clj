(ns ritual.core
  (:require [clojure.java.jdbc :as db]
            [pandect.core :refer [sha1]]))

;; ## Helpers

(defn sqlize
  "Convert string/symbol/keyword to string with dashes replaced by underscores."
  [s]
  (when s
    (-> ^String (if (or (symbol? s) (keyword? s))
                  (name s)
                  (str s))
        (.replace "-" "_"))))

;; ## Table Fixtures

(defn- infer-column-type
  "Determine the type for a column based on sample data."
  [data column]
  (let [column-values (->> (map #(get % column) data)
                           (filter identity))]
    (cond (every? number? column-values) :integer
          (every? string? column-values) (if (some #(> (count %) 255) column-values)
                                           :text
                                           "varchar(255)")
          :else :text)))

(defn- infer-columns
  "Derive the given columns based on sample data."
  [data primary-key]
  (->> (mapcat keys data)
       (cons primary-key)
       (filter identity)
       (distinct)
       (vec)))

(defn- ->create-table-format-string
  "Convert a seq of sample data to an SQL statement that will create the
   respective table."
  [data columns {:keys [primary-key defaults types]}]
  (let [pk (sqlize primary-key)]
    (->> (for [column columns
               :let [s (sqlize column)]]
           (->> (vector
                  s
                  (or (get types column)
                      (infer-column-type data column))
                  (when (= s pk) "PRIMARY KEY"))
                (filter identity)
                (vec)))
         (apply db/create-table-ddl "%s"))))

(defn- ->insert-function
  "Create function that inserts the given data using a database spec and a
   table name."
  [columns data]
  (let [column-names (mapv sqlize columns)
        data-rows (mapv (apply juxt columns) data)]
    #(apply db/insert! %1 %2 column-names data-rows)))

(defn- ->create-table-function
  "Create function that creates a table using a database spec and a table name."
  [data columns options]
  (let [fmt (->create-table-format-string data columns options)]
    #(db/execute! %1 [(format fmt %2)])))

(defn drop!
  "Create function that drops a table using a database spec and a table name."
  [db-spec table-key]
  (try
    (db/execute! db-spec [(db/drop-table-ddl (sqlize table-key))])
    (catch Throwable _)))

(defn- attach-table-metadata
  "Attach table metadata."
  [db-spec table-key primary-key columns]
  (->> {:primary-key primary-key
        :columns columns}
       (vary-meta db-spec assoc-in [::tables table-key])))

(defn- table-metadata
  "Access table metadata."
  [db-spec table-key k]
  (get-in (meta db-spec) [::tables table-key k]))

(defn table
  "Create DB table fixture from a given set of data. The result will be a function
   that takes database spec, as well as a table name, to create and fill the respective
   table. It will return a database spec that can be used to cleanup any created data."
  [data & {:keys [primary-key defaults types] :as options}]
  (let [columns (infer-columns data primary-key)
        create! (->create-table-function data columns options)
        insert! (->insert-function columns data)]
    (fn [db-spec table-key
         & {:keys [fresh? insert?]
            :or {fresh? true insert? true}}]
      (let [table (sqlize table-key)]
        (when fresh?
          (drop! db-spec table)
          (create! db-spec table))
        (when insert?
          (insert! db-spec table))
        (attach-table-metadata db-spec table-key primary-key columns)))))

;; ## Cleanup

(defn cleanup
  "Drop all created tables."
  [db-spec]
  (doseq [table-key (keys (::tables (meta db-spec)))]
    (drop! db-spec table-key))
  (vary-meta db-spec dissoc ::tables))

;; ## Snapshot Mechanism

(defn- ->select-query
  "Create query to select the given columns from the given table."
  [table columns]
  (vector
    (format "select %s from %s"
            (->> (map sqlize columns)
                 (distinct)
                 (clojure.string/join ","))
            (sqlize table))))

(defn- ->snapshot-pair
  "Create pair of primary key value and a hash over the given columns."
  [columns primary-key row]
  (let [pk (get row primary-key)
        cs (->> (sort-by sqlize columns)
                (map #(get row %))
                (map pr-str)
                (clojure.string/join ","))]
    [pk (sha1 cs)]))

(defn snapshot
  "Create snapshot of a specific table."
  ([db-spec table]
   (->> (table-metadata db-spec table :columns)
        (snapshot db-spec table)))
  ([db-spec table columns]
   (->> (or (table-metadata db-spec table :primary-key) (first columns))
        (snapshot db-spec table columns)))
  ([db-spec table columns primary-key]
   (let [columns (cons primary-key columns)]
     (assert (seq columns) "no valid columns given.")
     (assert primary-key "no valid primary key given.")
     (->> (db/query db-spec (->select-query table columns)
                    :row-fn    #(->snapshot-pair columns primary-key %)
                    :result-fn #(into {} %))
          (into {})))))

(defn diff
  "Compare two snapshots."
  [snapshot-a snapshot-b]
  (letfn [(lazy-diff [sa sb]
            (lazy-seq
              (cond (empty? sa) (map #(vector :insert (first %)) sb)
                    (empty? sb) (map #(vector :delete (first %)) sa)
                    :else (let [[[ka ha] & ra] sa
                                [[kb hb] & rb] sb
                                c (compare ka kb)]
                            (cond (neg? c) (cons [:delete ka] (lazy-diff ra sb))
                                  (pos? c) (cons [:insert kb] (lazy-diff sa rb))
                                  (= ha hb) (lazy-diff ra rb)
                                  :else (cons [:update ka] (lazy-diff ra rb)))))))]
    (lazy-diff
      (sort-by first snapshot-a)
      (sort-by first snapshot-b))))
