(ns kxygk.airport
  (:require [tock]
            [tick.core                    :as tick]
            [tick.locale-en-us]
            [tick.alpha.interval          :as interval]
            [quickthing]
            [dk.ative.docjure.spreadsheet :as docjure]
            [tech.v3.dataset              :as ds]
            [tech.v3.libs.fastexcel]
            #_[injest.classical]
            #_[clojure.data.csv]))

#_
(def xlsx-filestr
  #_
  "/home/kxygk/Data/airport/first-sheet-fixed-dates-two.xls"
  ;;#_
  "/home/kxygk/Data/airport/first-sheet-fixed-dates.xlsx")

(def isotope-csv-filestr
  "/home/kxygk/Data/airport/first-sheet-extracted.csv")

(def climate-csv-filestr
  "/home/kxygk/Projects/imergination.wiki/krabdaily/climate-index.csv")

(def isotopes ;;table
  (ds/->dataset  isotope-csv-filestr
                 #_xlsx-filestr
                 {:column-whitelist ["Date"
                                     "Rain (mm)"
                                     "d18O"
                                     "dD"
                                     "Comment"]
                  :parser-fn {"Date" #_string
                              [:local-date "yyyy-MM-dd"]}}))
#_
(identity isotopes)
;; => /home/kxygk/Data/airport/first-sheet-extracted.csv [2282 5]:
;;    |       Date | Rain (mm) |  d18O |     dD | Comment |
;;    |------------|-----------|-------|-------:|---------|
;;    | 2010-02-02 |       0.5 | -0.99 |  -0.20 |         |
;;    | 2010-02-10 |       8.0 | -5.37 | -31.60 |         |
;;    | 2010-07-20 |      27.4 | -1.26 |   2.45 |         |
;;    | 2010-07-24 |       0.5 | -3.05 | -27.84 |         |
;;    | 2010-07-25 |       3.0 | -4.94 | -27.23 |         |
;;    | 2010-07-26 |      14.5 | -2.98 | -11.08 |         |
;;    | 2010-07-27 |       1.8 |  -6.4 | -38.32 |         |
;;    | 2010-07-29 |      37.0 | -6.31 | -35.24 |         |
;;    | 2010-07-30 |      66.0 | -1.84 |   2.88 |         |
;;    | 2010-07-31 |      20.2 | -7.23 | -47.94 |         |
;;    |        ... |       ... |   ... |    ... |     ... |
;;    | 2023-12-30 |     7.4mm | -3.20 | -16.97 |         |
;;    | 2024-01-18 |     0.6mm | -3.54 | -33.12 |         |
;;    | 2024-01-22 |     0.6mm | -6.79 | -49.15 |         |
;;    | 2024-01-23 |     0.2mm |  0.62 |   6.00 |         |
;;    | 2024-02-06 |     0.2mm |  0.42 |   5.77 |         |
;;    | 2024-02-08 |     0.2mm |  1.56 |   0.51 |         |
;;    | 2024-03-30 |      15mm | -6.63 | -49.26 |         |
;;    | 2024-04-20 |     1.8mm | -0.81 |   1.83 |         |
;;    | 2024-04-24 |     2.4mm | -0.87 |   5.24 |         |
;;    | 2024-04-27 |     0.2mm | -3.87 | -29.50 |         |
;;    | 2024-04-28 |     3.2mm | -3.56 | -14.57 |         |



(defn
  gen-dates
  [start-date
   end-date]
  (->> (tick/range
         (tick/instant start-date)
         (tick/instant end-date ) ;; doesn't include last value
         (tick/new-period 1 :days)) #_
       (mapv tick/date)
       (mapv #(tick/in %
                       "UTC"))
       (mapv #(tick/format (tick/formatter "yyyy-MM-dd")
                           %))))
#_
(gen-dates #inst"2011-01-01"
           #inst"2011-01-15")
;; => ["2011-01-01"
;;     "2011-01-02"
;;     "2011-01-03"
;;     "2011-01-04"
;;     "2011-01-05"
;;     "2011-01-06"
;;     "2011-01-07"
;;     "2011-01-08"
;;     "2011-01-09"
;;     "2011-01-10"
;;     "2011-01-11"
;;     "2011-01-12"
;;     "2011-01-13"
;;     "2011-01-14"]

(defn
  leap-day?
  [date]
  (and (= (tick/day-of-month date)
          29)
       (= (tick/month date)
          tick/FEBRUARY)))
#_
(leap-day? (tick/instant #inst"2012-02-29"))
  
(defn
  remove-leapdays
  [dates]
  (->> dates
       (filter #(-> %
                    leap-day?
                    not))))
#_
(-> (gen-dates #inst"2011-01-01"
               #inst"2021-01-01")
    remove-leapdays
    count)
;; => 3650

(def dates
  (ds/->dataset (->> (gen-dates #inst"2011-01-01"
                                #inst"2021-01-01")
                     remove-leapdays
                     (mapv #(assoc {}
                                   "Date"
                                   %)))
                {:parser-fn {"Date" [:local-date "yyyy-MM-dd"]}}))
#_
(identity dates)




(def climate
  (assoc (ds/->dataset climate-csv-filestr
                       {:header-row? false})
         "Date"
         (get dates
            "Date")))
#_
(identity climate)
;; => /home/kxygk/Projects/imergination.wiki/krabdaily/climate-index.csv [3650 3]:
;;    | column-0 |   column-1 |       Date |
;;    |---------:|-----------:|------------|
;;    |      0.0 | 0.00383854 | 2011-01-01 |
;;    |      0.0 | 0.00197601 | 2011-01-02 |
;;    |      0.0 | 0.00230665 | 2011-01-03 |
;;    |      0.0 | 0.00176155 | 2011-01-04 |
;;    |      0.0 | 0.03206110 | 2011-01-05 |
;;    |      0.0 | 0.03051165 | 2011-01-06 |
;;    |      0.0 | 0.00880878 | 2011-01-07 |
;;    |      0.0 | 0.01147761 | 2011-01-08 |
;;    |      0.0 | 0.00184225 | 2011-01-09 |
;;    |      0.0 | 0.00384430 | 2011-01-10 |
;;    |      ... |        ... |        ... |
;;    |      0.0 | 0.00036548 | 2020-12-21 |
;;    |      0.0 | 0.00009164 | 2020-12-22 |
;;    |      0.0 | 0.00274088 | 2020-12-23 |
;;    |      0.0 | 0.06215764 | 2020-12-24 |
;;    |      0.0 | 0.01916776 | 2020-12-25 |
;;    |      0.0 | 0.00876432 | 2020-12-26 |
;;    |      0.0 | 0.00134713 | 2020-12-27 |
;;    |      0.0 | 0.00023292 | 2020-12-28 |
;;    |      0.0 | 0.00035076 | 2020-12-29 |
;;    |      0.0 | 0.00150992 | 2020-12-30 |
;;    |      0.0 | 0.00168473 | 2020-12-31 |
#_
(ds/row-count climate)
;; => 3650
