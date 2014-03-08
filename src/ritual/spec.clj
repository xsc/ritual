(ns ritual.spec
  (:require [ritual.utils :refer [sql-keyword]]))

;; ## Helpers

(defn- ->key
  "Create key vector, prefixed with `::spec`."
  [k]
  (if (keyword? k)
    [::spec k]
    (vec (cons ::spec k))))

(defn- assoc-meta
  "Assoc the given key/value in the metadata."
  [db-spec k v]
  (vary-meta db-spec assoc-in (->key k) v))

(defn- update-meta
  "Update the given key/value in the metadata."
  [db-spec k f & args]
  (vary-meta db-spec update-in (->key k) #(apply f % args)))

(defn- get-meta
  "Access metadata."
  [db-spec k]
  (get-in (meta db-spec) (->key k)))

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

(defn set-cleanup-conditions
  "Set cleanup conditions for the given table."
  [db-spec table c]
  (assoc-meta db-spec [:tables (sql-keyword table) :cleanup] c))

(defn cleanup-conditions
  "Get cleanup conditions for the given table."
  [db-spec table]
  (get-meta db-spec [:tables (sql-keyword table) :cleanup]))

(defn cleanup-include
  "Add cleanup conditions."
  [db-spec table column values]
  (let [vs (if (sequential? values) values [values])]
    (update-meta db-spec [:tables (sql-keyword table) :cleanup]
                 (fn [cleanup]
                   (cond (= cleanup :none) (hash-map column vs)
                         (keyword? cleanup) cleanup
                         :else (update-in cleanup [column]
                                         (comp vec distinct concat) vs))))))

(defn tables
  "Get all observed tables."
  [db-spec]
  (keys (get-meta db-spec [:tables])))
