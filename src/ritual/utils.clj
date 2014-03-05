(ns ritual.utils
  (:require [clojure.string :refer [lower-case]]))

(defn sqlize
  "Convert string/symbol/keyword to string with dashes replaced by underscores."
  [s]
  (when s
    (-> ^String (if (or (symbol? s) (keyword? s))
                  (name s)
                  (str s))
        (.replace "-" "_")
        (lower-case))))

(defn sql-keyword
  "Convert string/symbol/keyword to sqlized keyword."
  [k]
  (keyword (sqlize k)))
