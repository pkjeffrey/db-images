(ns db-images.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io])
  (:import (java.io ByteArrayOutputStream)))

(def db-spec "jdbc:derby://localhost:1527/music")
(def files-path "/home/peter/db/releases/")

(defn file->bytearray [f]
  (with-open [input (io/input-stream f)
              buffer (ByteArrayOutputStream.)]
    (io/copy input buffer)
    (.toByteArray buffer)))

(defn db-update [n f]
  (jdbc/execute! db-spec
                 ["update release set art = ? where id = ?" (file->bytearray f) n]))

(defn update-file [n]
  (let [f (str files-path n ".jpg")]
    (when (.exists (io/as-file f))
      (db-update n f))))

(defn get-ids []
  (map #(:id %) (jdbc/query db-spec ["select id from release"])))

(defn run []
  (loop [ids (get-ids)]
    (when-not (empty? ids)
      (update-file (first ids))
      (recur (rest ids)))))