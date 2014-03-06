(ns ritual.types-test
  (:require [midje.sweet :refer :all]
            [ritual.types :refer :all]))

(tabular
  (fact "about initial type inference"
        (column-type nil ?next-data) => ?type)
  ?next-data                   ?type
  0                            :integer
  0.0                          :float
  0N                           :biginteger
  0.0M                         :bigdecimal
  (java.util.Date.)            :date
  "0"                          :string
  (apply str (repeat 1000 \0)) :text
  true                         :boolean
  false                        :boolean)

(tabular
  (fact "about integer type inference"
        (column-type :integer ?next-data) => ?type)
  ?next-data                   ?type
  0                            :integer
  0.0                          :float
  0N                           :biginteger
  0.0M                         :bigdecimal
  (java.util.Date.)            :unknown
  "0"                          :unknown
  (apply str (repeat 1000 \0)) :unknown
  true                         :unknown
  false                        :unknown)

(tabular
  (fact "about float type inference"
        (column-type :float ?next-data) => ?type)
  ?next-data                   ?type
  0                            :float
  0.0                          :float
  0N                           :bigdecimal
  0.0M                         :bigdecimal
  (java.util.Date.)            :unknown
  "0"                          :unknown
  (apply str (repeat 1000 \0)) :unknown
  true                         :unknown
  false                        :unknown)

(tabular
  (fact "about biginteger type inference"
        (column-type :biginteger ?next-data) => ?type)
  ?next-data                   ?type
  0                            :biginteger
  0.0                          :bigdecimal
  0N                           :biginteger
  0.0M                         :bigdecimal
  (java.util.Date.)            :unknown
  "0"                          :unknown
  (apply str (repeat 1000 \0)) :unknown
  true                         :unknown
  false                        :unknown)

(tabular
  (fact "about bigdecimal type inference"
        (column-type :bigdecimal ?next-data) => ?type)
  ?next-data                   ?type
  0                            :bigdecimal
  0.0                          :bigdecimal
  0N                           :bigdecimal
  0.0M                         :bigdecimal
  (java.util.Date.)            :unknown
  "0"                          :unknown
  (apply str (repeat 1000 \0)) :unknown
  true                         :unknown
  false                        :unknown)

(tabular
  (fact "about date type inference"
        (column-type :date ?next-data) => ?type)
  ?next-data                   ?type
  0                            :unknown
  0.0                          :unknown
  0N                           :unknown
  0.0M                         :unknown
  (java.util.Date.)            :date
  "0"                          :unknown
  (apply str (repeat 1000 \0)) :unknown
  true                         :unknown
  false                        :unknown)

(tabular
  (fact "about string type inference"
        (column-type :string ?next-data) => ?type)
  ?next-data                   ?type
  0                            :unknown
  0.0                          :unknown
  0N                           :unknown
  0.0M                         :unknown
  (java.util.Date.)            :unknown
  "0"                          :string
  (apply str (repeat 1000 \0)) :text
  true                         :unknown
  false                        :unknown)

(tabular
  (fact "about text type inference"
        (column-type :text ?next-data) => ?type)
  ?next-data                   ?type
  0                            :unknown
  0.0                          :unknown
  0N                           :unknown
  0.0M                         :unknown
  (java.util.Date.)            :unknown
  "0"                          :text
  (apply str (repeat 1000 \0)) :text
  true                         :unknown
  false                        :unknown)

(tabular
  (fact "about boolean type inference"
        (column-type :boolean ?next-data) => ?type)
  ?next-data                   ?type
  0                            :unknown
  0.0                          :unknown
  0N                           :unknown
  0.0M                         :unknown
  (java.util.Date.)            :unknown
  "0"                          :unknown
  (apply str (repeat 1000 \0)) :unknown
  true                         :boolean
  false                        :boolean)
