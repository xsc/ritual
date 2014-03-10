(ns ritual.spec
  (:require [ritual.utils :refer [sql-keyword]]))

;; ## Helpers

(defn- ->key
  "Create key vector, prefixed with `::spec`."
  [k]
  (if (keyword? k)
    [::data k]
    (vec (cons ::data k))))

(defn- assoc-meta
  "Assoc the given key/value into the DB spec."
  [db-spec k v]
  (assoc-in db-spec (->key k) v))

(defn- update-meta
  "Update the given key/value in the metadata."
  [db-spec k f & args]
  (update-in db-spec (->key k) #(apply f % args)))

(defn- get-meta
  "Access metadata."
  [db-spec k]
  (get-in db-spec (->key k)))

;; ## Data

(defn set-primary-key
  "Set the primary key for the given table."
  [db-spec table primary-key]
  (let [tk (sql-keyword table)
        pk (sql-keyword primary-key)]
    (-> db-spec
        (assoc-meta [:tables tk :primary-key] pk)
        (update-meta [:tables tk :columns] (comp set conj) primary-key))))

(defn primary-key
  "Get the primary key for the given table."
  [db-spec table]
  (get-meta db-spec [:tables (sql-keyword table) :primary-key]))

(defn set-options
  "Set initialization options for the given table."
  [db-spec table options]
  (assoc-meta db-spec [:tables (sql-keyword table) :options] options))

(defn options
  "Get initialization options for the given table"
  [db-spec table]
  (get-meta db-spec [:tables (sql-keyword table) :options]))

(defn set-columns
  "Set the observed columns for the given table."
  [db-spec table columns]
  (->> (mapv sql-keyword columns)
       (set)
       (assoc-meta db-spec [:tables (sql-keyword table) :columns])))

(defn columns
  "Get the observed columns for the given table."
  [db-spec table]
  (get-meta db-spec [:tables (sql-keyword table) :columns]))

;; ## Cleanup

(defn- merge-cleanup-conditions
  [cond-a cond-b]
  (cond (not cond-a) cond-b
        (not cond-b) cond-a
        (= cond-a cond-b) cond-a
        (or (= cond-a :drop) (= cond-b :drop)) :drop
        (or (= cond-a :clear) (= cond-b :clear)) :clear
        (= cond-a :none) cond-b
        (= cond-b :none) cond-a
        :else (merge-with (comp vec distinct concat) cond-a cond-b)))

(defn add-cleanup-conditions
  "Add cleanup conditions for the given table."
  [db-spec table c]
  (update-meta db-spec [:tables (sql-keyword table) :cleanup] merge-cleanup-conditions c))

(defn cleanup-conditions
  "Get cleanup conditions for the given table."
  [db-spec table]
  (get-meta db-spec [:tables (sql-keyword table) :cleanup]))

(defn cleanup-include
  "Add cleanup conditions."
  [db-spec table column values]
  (let [vs (if (sequential? values) values [values])]
    (update-meta db-spec [:tables (sql-keyword table) :cleanup]
                 merge-cleanup-conditions (hash-map column vs))))

;; ## Access

(defn tables
  "Get all observed tables."
  [db-spec]
  (keys (get-meta db-spec [:tables])))
