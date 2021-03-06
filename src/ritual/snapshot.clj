(ns ritual.snapshot
  (:require [ritual.spec :as spec]
            [ritual.utils :refer [sqlize sql-keyword]]
            [pandect.core :refer [sha1]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [clojure.set :refer [intersection]]))

;; ## Snapshot SQL

(defn- snapshot-condition
  "Create 'IN' or '=' condition for snapshot query."
  [k vs]
  (when (and k (seq vs))
    (if (= (count vs) 1)
      (format "%s = ?" (sqlize k))
      (format "%s in (%s)"
              (sqlize k)
              (s/join "," (repeat (count vs) "?"))))))

(defn- snapshot-where
  "Create WHERE clause for snapshot query."
  [conditions]
  (let [pairs (for [[k vs] (partition 2 conditions)]
                [k (if (coll? vs) vs [vs])])
        values (apply concat (map second pairs))]
    (when (seq values)
      (when-let [parts (seq
                         (keep
                           (fn [[k vs]]
                             (snapshot-condition k vs))
                           pairs))]
        (vector
          (str "where " (s/join " and " parts))
          values)))))

(defn snapshot-query
  "Create query to retrieve the given columns from the given table."
  [table columns & conditions]
  (let [base-clause (format "select %s from %s"
                            (if (seq columns)
                              (->> (map sqlize columns)
                                   (filter identity)
                                   (distinct)
                                   (s/join ","))
                              "*")
                            (sqlize table))
        [where-clause values] (snapshot-where conditions)
        query-string (->> [base-clause where-clause]
                          (filter identity)
                          (s/join " "))]
    (vec (list* query-string values))))

(defn select-snapshot!
  "Create snapshot map for the given table.."
  [db-spec table columns primary-key conditions & {:keys [row-fn]}]
  (assert primary-key "no valid primary key given for snapshot.")
  (let [cols (when (seq columns)
               (->> columns
                    (cons primary-key)
                    (filter identity)
                    (distinct)
                    (mapv sql-keyword)))
        pk (sql-keyword primary-key)
        query (apply snapshot-query table cols conditions)
        f (or row-fn identity)]
    (jdbc/query db-spec query
                :row-fn (fn [row]
                          (when-let [pk-value (get row pk)]
                            [pk-value (f row)]))
                :identifiers sql-keyword
                :result-set-fn #(into {} %))))

;; ## DB Access

(defn- snapshot-hash
  "Create SHA-1 hash of the given row."
  [columns row]
  (let [cols (or (seq columns) (sort-by sqlize (keys row)))]
    (->> (map #(get row %) cols)
         (map pr-str)
         (s/join ",")
         (sha1))))

(defn- prepare-columns
  "Create sorted seq of columns, based on the given DB spec and filter."
  [db-spec table only]
  (->> (let [cols (set (spec/columns db-spec table))]
         (if (seq only)
           (intersection cols (set only))
           cols))
       (sort-by sqlize)))

(defn- prepare-conditions
  [opts]
  (->> (dissoc opts :only :by)
       (apply concat)))

(defn- prepare-primary-key
  "Prepare the primary key for the given table, based on the given DB spec."
  [db-spec table by]
  (or by (spec/primary-key db-spec table)))

(defn snapshot
  "Create snapshot map for the given table, associating the primary key value
   with a hash of the respective row."
  [db-spec table & {:keys [only by] :as opts}]
  (let [conditions (prepare-conditions opts)
        columns (prepare-columns db-spec table only)
        primary-key (prepare-primary-key db-spec table by)
        hash-columns (->> (map sql-keyword only)
                          (distinct)
                          (sort-by sqlize)
                          (or (seq columns)))]
    (select-snapshot!
      db-spec table columns primary-key conditions
      :row-fn #(snapshot-hash hash-columns %))))

(defn dump
  "Create dump for the given table, associating the primary key value
   with the observed columns of the given row."
  [db-spec table & {:keys [only by] :as opts}]
  (let [conditions (prepare-conditions opts)
        columns (prepare-columns db-spec table only)
        primary-key (prepare-primary-key db-spec table by)]
    (select-snapshot! db-spec table columns primary-key conditions)))

;; ## Diff

(defn diff
  "Compare two snapshots."
  ([snapshot-a snapshot-b]
   (diff compare snapshot-a snapshot-b))
  ([primary-key-comp snapshot-a snapshot-b]
   (letfn [(lazy-diff [sa sb]
             (lazy-seq
               (cond (empty? sa) (map #(vector :insert (first %)) sb)
                     (empty? sb) (map #(vector :delete (first %)) sa)
                     :else (let [[[ka ha] & ra] sa
                                 [[kb hb] & rb] sb
                                 c (primary-key-comp ka kb)]
                             (cond (neg? c) (cons [:delete ka] (lazy-diff ra sb))
                                   (pos? c) (cons [:insert kb] (lazy-diff sa rb))
                                   (= ha hb) (lazy-diff ra rb)
                                   :else (cons [:update ka] (lazy-diff ra rb)))))))]
     (lazy-diff
       (sort-by first snapshot-a)
       (sort-by first snapshot-b)))))
