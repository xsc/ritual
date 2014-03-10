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
   snapshot dump diff]
  [ritual.spec
   cleanup-include])

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
  [cleanup-by data table-created? primary-key]
  (if cleanup-by
    (or
      (#{:drop :clear :none} cleanup-by)
      (let [c (if (sequential? cleanup-by) cleanup-by [cleanup-by])]
        (->> (for [cleanup-key c]
               (->> (map #(get % cleanup-key) data)
                    (filter identity)
                    (distinct)
                    (vec)
                    (vector cleanup-key)))
             (into {}))))
    (if table-created?
      :drop
      (recur [primary-key] data table-created? primary-key))))

;; ## Table Fixture

(defn table
  "Create DB table fixture from a given set of data. The result will be a function
   that takes database spec, as well as a table name, to create and fill the respective
   table. It will return a database spec that can be used to clean up any created data.

   - `:primary-key`: the field that will be set primary key,
   - `:overrides`: type/index overrides for database columns,
   - `:cleanup-by`: vector of columns that will be used for cleanup.

   Cleanup is done by collecting all values for the given columns and deleting all
   rows that match the given columns.
   "
  [data & {:keys [primary-key overrides]}]
  (let [columns (collect-columns data primary-key overrides)
        column-types (infer-types columns data)]
    (fn [db-spec table-key & {:keys [force? insert? cleanup] :as options}]
      (let [{:keys [force? insert?] :as options} (merge
                                                   {:force? false
                                                    :insert? true}
                                                   options)
            exists? (table/exists? db-spec table-key)
            drop? (and exists? force?)
            create? (or (not exists?) drop?)
            cleanup-conditions (collect-cleanup-conditions cleanup data create? primary-key)]

        (when drop?
          (table/drop! db-spec table-key))

        (when create?
          (table/create! db-spec table-key column-types primary-key overrides))

        (when insert?
          (table/insert! db-spec table-key column-types data))

        (-> db-spec
            (spec/add-cleanup-conditions table-key cleanup-conditions)
            (spec/set-primary-key table-key primary-key)
            (spec/set-columns table-key columns)
            (spec/set-options table-key options))))))

(defn table!
  "Create table fixture directly. (see `table` for options)"
  [db-spec table-key & args]
  (let [f (apply table args)]
    (f db-spec table-key)))

;; ## Cleanup

(defn cleanup
  "Cleanup all table fixtures associated with the given database spec. This will

   - drop tables if `:force?` was set on creation,
   - delete rows if `:cleanup-by` is set to `:clear`,
   - delete rows using the conditions specified in `:cleanup-by`.
   "
  [db-spec]
  (doseq [t (spec/tables db-spec)]
    (let [cleanup (spec/cleanup-conditions db-spec t)]
      (cond (or (not cleanup) (= cleanup :drop)) (table/drop-if-exists! db-spec t)
            (= cleanup :none) nil
            (= cleanup :clear) (table/clean! db-spec t nil)
            :else (table/clean! db-spec t cleanup))))
  (vary-meta db-spec dissoc ::spec))

;; ## Tracing

(defn- trace-query
  "Print the query vector."
  [tag [_ & rst]]
  (printf "%s %s%n" tag (pr-str (vec rst))))

(defn- wrap-trace
  "Wrap the given query execution function to print each query."
  [f tag]
  (if-not (-> f meta ::trace)
    (-> (fn [& args]
          (trace-query tag args)
          (apply f args))
        (vary-meta assoc ::trace f))
    f))

(defn- unwrap-trace
  "Unwrap the given query."
  [f]
  (or (-> f meta ::trace) f))

(defn trace!
  "Activate SQL tracing."
  []
  (alter-var-root #'jdbc/execute! wrap-trace "[jdbc/execute!]")
  (alter-var-root #'jdbc/insert! wrap-trace "[jdbc/insert!] ")
  (alter-var-root #'jdbc/delete! wrap-trace "[jdbc/delete!] ")
  (alter-var-root #'jdbc/update! wrap-trace "[jdbc/update!] ")
  (alter-var-root #'jdbc/query wrap-trace "[jdbc/query]   "))

(defn untrace!
  "Deactivate SQL tracing."
  []
  (doseq [v [#'jdbc/execute! #'jdbc/insert! #'jdbc/delete!
             #'jdbc/update! #'jdbc/query]]
    (alter-var-root v unwrap-trace)))
