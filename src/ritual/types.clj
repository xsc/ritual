(ns ritual.types)

;; ## Type Inference

(defn- class-key
  "Get keyword representing the given value's type."
  [v]
  (cond (instance? clojure.lang.BigInt v) :biginteger
        (instance? java.math.BigDecimal v) :bigdecimal
        (instance? java.util.Date v) :date
        (integer? v) :integer
        (float? v) :float
        (string? v) (if (< (count v) 256)
                      :string
                      :text)
        (instance? java.lang.Boolean v) :boolean
        :else :unknown))

(defmulti column-type
  "infer column type from the current one and the next piece of data."
  (fn [current-type next-data]
    (cond (nil? next-data) :keep
          (not current-type) :first
          :else (let [t (class-key next-data)]
                  (if (= t current-type)
                    :keep
                    (hash-set current-type t)))))
  :default :unknown)

(defmethod column-type :unknown [current-type next-data] :unknown)
(defmethod column-type :keep [current-type _] current-type)
(defmethod column-type :first [_ next-data] (class-key next-data))
(defmethod column-type #{:biginteger :integer} [& _] :biginteger)
(defmethod column-type #{:text :string} [& _] :text)
(defmethod column-type #{:integer :float} [& _] :float)
(defmethod column-type #{:float :biginteger} [& _] :bigdecimal)
(defmethod column-type #{:float :bigdecimal} [& _] :bigdecimal)
(defmethod column-type #{:integer :bigdecimal} [& _] :bigdecimal)
(defmethod column-type #{:biginteger :bigdecimal} [& _] :bigdecimal)

(defn- update-column-types
  "Perform a single inference step."
  [column-types row]
  (->> (for [[column current-type] column-types]
         (when-let [new-type (column-type current-type (get row column))]
           [column new-type]))
       (into column-types)))

(defn infer-types
  "Infer column types based on a seq of columns and the given data."
  [columns data]
  (let [initial-types (zipmap columns (repeat nil))]
    (reduce update-column-types initial-types data)))

;; ## Type Mappings

(def default-types
  "Types for most SQL databases."
  {:text       "varchar(32672)"
   :string     "varchar(255)"
   :integer    "integer"
   :float      "double"
   :biginteger "bigint"
   :bigdecimal "decimal"
   :date       "date"
   :unknown    "blob"})

(def mysql-types
  "Types for MySQL."
  {:text       "text"
   :string     "varchar(255)"
   :integer    "integer"
   :float      "double"
   :biginteger "bigint"
   :bigdecimal "decimal"
   :date       "date"
   :unknown    "blob"})

(def ^:dynamic *type-mapping*
  "Types to use for mapping the abstract ones to
   DB-specific ones."
  default-types)
