(ns ritual.db
  (:require [clojure.java.jdbc :as jdbc]))

(defn create-derby
  "Create Derby in-memory database."
  [k]
  {:subprotocol "derby"
   :subname (format "memory:test:%s" (name k))
   :create true})
