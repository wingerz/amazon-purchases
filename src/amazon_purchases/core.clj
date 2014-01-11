(ns amazon-purchases.core
  (:import java.io.File)
  (:require [clojure-csv.core :as csv])
  (:gen-class))

(defn get-data-filenames
  [dir]
  (filter (fn [x] (re-find #".csv$" x))
          (for [file (file-seq (File. dir))] (.getPath file))))

(defn normalize-header [header]
  (->
   header
   clojure.string/lower-case
   (.replace " " "")
   keyword))

(defn parse-amazon-data [raw-data]
  (let
      [csv-data (csv/parse-csv raw-data)
       header (first csv-data)
       rows (rest csv-data)
       ]
    (map
     (fn [row]
       (zipmap
        (map normalize-header header)
        row))
     rows)))

(defn from-dollar-amount [dollar-str]
  (read-string (second (clojure.string/split dollar-str #"\$"))))

(defn from-date [date-str]
  (let [mdy (map (fn [x] (Integer/parseInt x)) (clojure.string/split date-str #"/"))
        rawyear (last mdy)
        month (first mdy)
        year (cond (> rawyear 90) (+ rawyear 1900)
                   :else (+ rawyear 2000)
                   )]
    (vec [year month])))

(defn adapt-item [raw-item]
  (assoc raw-item
    :perunitprice (from-dollar-amount (:perunitprice raw-item) )
    :orderdate (from-date (:orderdate raw-item) )
    ))

(defn summarize-by-month [data]
  (sort-by key (reduce #(assoc %1 %2 (inc (%1 %2 0)))
          {}
          (map :orderdate data))))
  
(defn summarize-by-category [data]
  (sort-by val > (reduce #(assoc %1 %2 (inc (%1 %2 0)))
          {}
          (map :category data))))

(defn parse-file [csv-file]
  (parse-amazon-data (slurp csv-file)))

(defn summarize [data]
  (hash-map
   :bycategory (summarize-by-category data)
   :bymonth (summarize-by-month data)
   :total (count data)
   )
  )

(defn get-paths [dir]
  (filter
   (fn [x] (re-find #".csv$" x))
   (for [file (file-seq (File. dir))] (str (.getPath file)))))

(defn process [dir]
  (let
      [
       data-files (get-paths dir)
       data (flatten (map parse-file data-files))
       summary (summarize (map adapt-item data))
       ]
    summary
    )
  )

(defn -main [& args]
  (println (process (first args))) 
  )