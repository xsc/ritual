(ns ritual.core
  (:require [ritual
             [snapshot]
             [table :as table]
             [types :refer [infer-types]]
             [spec :as spec]
             [utils :refer [sql-keyword sqlize]]]
            [clojure.java.jdbc :as jdbc]
            [potemkin :refer [import-vars]]))

;; ## Import API

(import-vars
  [ritual.snapshot
   snapshot dump])

;; ## Data

(defn- collect-columns
  "Derive the given columns based on sample data."
  [data primary-key overrides]
  (->> (mapcat keys data)
       (cons primary-key)
       (concat (keys overrides))
       (filter identity)
       (distinct)
       (vec)))

(defn- collect-cleanup-conditions
  "Create cleanup condition map."
  [cleanup-by data]
  (or
    (when-not cleanup-by :all)
    (#{:all :none} cleanup-by)
    (->> (for [cleanup-key cleanup-by]
         (->> (map #(get % cleanup-key) data)
              (filter identity)
              (distinct)
              (vec)
              (vector cleanup-key)))
       (into {}))))

;; ## Table Fixture

(defn table
  "Create DB table fixture from a given set of data. The result will be a function
   that takes database spec, as well as a table name, to create and fill the respective
   table. It will return a database spec that can be used to cleanup any created data."
  [data & {:keys [primary-key overrides cleanup-by]}]
  (let [columns (collect-columns data primary-key overrides)
        column-types (infer-types columns data)
        cleanup-conditions (collect-cleanup-conditions cleanup-by data)]
    (fn [db-spec table-key & {:keys [force? insert?] :as options}]
      (let [{:keys [force? insert?] :as options} (-> (merge
                                                       {:force? true
                                                        :insert? true}
                                                       options)
                                                     (assoc :cleanup cleanup-conditions))]
        (when force?
          (table/drop-if-exists! db-spec table-key)
          (table/create! db-spec table-key column-types primary-key overrides))
        (when-not force?
          (table/create-if-not-exists! db-spec table-key column-types primary-key overrides))
        (when insert?
          (table/insert! db-spec table-key column-types data))
        (-> db-spec
            (spec/set-primary-key table-key primary-key)
            (spec/set-columns table-key columns)
            (spec/set-options table-key options))))))

;; ## Cleanup

(defn cleanup
  "Cleanup all table fixtures associated with the given database spec. This will

   - drop tables if `:force?` was set on creation,
   - delete rows if `:cleanup-by` is set to `:all`,
   - delete rows using the conditions specified in `:cleanup-by`.
   "
  [db-spec]
  (doseq [t (spec/tables db-spec)]
    (let [{:keys [force? cleanup]} (spec/options db-spec t)]
      (cond force? (table/drop-if-exists! db-spec t)
            (= cleanup :none) nil
            (= cleanup :all) (table/clean! db-spec t nil)
            :else (table/clean! db-spec t cleanup))))
  (vary-meta db-spec dissoc ::spec))

;; ## Tracing

(defn- trace-query
  [[_ query & _]]
  (println query))

(defn- wrap-trace
  [f]
  (if-not (-> f meta ::trace)
    (-> (fn [& args]
          (trace-query args)
          (apply f args))
        (vary-meta assoc ::trace f))
    f))

(defn trace!
  []
  (alter-var-root #'jdbc/execute! wrap-trace)
  (alter-var-root #'jdbc/query wrap-trace))
