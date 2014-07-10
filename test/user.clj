(ns user
  (:refer-clojure :exclude [read-string])
  (:require [clojure.edn       :refer [read-string]]
            [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [com.stuartsierra.component :as component]
            [clojure.java.io :refer [file]]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :refer [chan put! <! <!! go close!]])
  (:import [com.stuartsierra.component Lifecycle]))

(def default-exchange-name "")
(def qname "langohr.examples.hello-world")
(declare system)

(defn take-chan [langohr-chan]
  (let [ch (chan 1)]
    (lc/subscribe (:langohr-chan langohr-chan) qname
                  (fn [_ {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
                    (put! ch (String. payload "UTF-8"))) :auto-ack true)
    ch))

(defn put-chan [langohr-chan]
  (let [ch (chan 1)]
    (go (loop []
          (let [msg (<! ch)]
            (when-not (nil? msg)
              (lb/publish (:langohr-chan langohr-chan)
                          default-exchange-name qname msg
                          :content-type "text/plain" :type "greetings.hi")
              (recur)))))
    ch))

(defrecord LangohrConn [config]
  component/Lifecycle
  (start [this]
    (assoc this :langohr-conn (rmq/connect config)))
  (stop [this]
    (rmq/close (:langohr-conn this))
    (assoc this :langohr-conn nil)))

(defrecord LangohrChan [langohr-conn]
  component/Lifecycle
  (start [this]
    (assoc this :langohr-chan
           (let [rch (lch/open (:langohr-conn langohr-conn))]
             (lq/declare rch qname :exclusive false :auto-delete true)
             rch)))
  (stop [this]
    (rmq/close (:langohr-chan this))
    (assoc this
      :langohr-chan nil
      :langohr-conn nil)))


(defrecord LangohrSubscribedChan [langohr-chan]
  component/Lifecycle
  (start [this]
    (assoc this :langohr-subscribed-chan
           (take-chan langohr-chan)))
  (stop [this]
    (close! (:langohr-subscribed-chan this))
    (assoc this
      :langohr-chan nil
      :langohr-subscribed-chan nil)))

(defn langohr-system [config]
  (component/system-map
   :langohr-conn (map->LangohrConn
                  {:config (merge rmq/*default-config* config)})
   :langohr-chan (component/using
                  (map->LangohrChan {})
                  [:langohr-conn])
   :langohr-subscribed-chan (component/using
                             (map->LangohrSubscribedChan {})
                             [:langohr-chan])))



(defn start-langohr
  "connect to rabbitmq"
  []
  (let [conf "conf/q.clj"]
    (assert (.exists (file conf)) (str "configuration file not found: " conf))
    (def system nil)
    (alter-var-root #'system (constantly (langohr-system (read-string (slurp conf)))))
    (alter-var-root #'system component/start)))

(defn stop-langohr
  "disconnect from rabbitmq: close connection, cleanup"
  [] (alter-var-root #'system component/stop))

(defn start-listening
  "start listening to messages from rabbitmq, and print each on stdout
   must be called after start-langohr"
  []
  (go (loop []
        (let [msg (<! (get-in system [:langohr-subscribed-chan :langohr-subscribed-chan]))]
          (when-not (nil? msg)
            (println "Got msg:" msg)
            (flush)
            (recur))))))

(defn send-message [msg]
  (let [ch (put-chan (:langohr-chan system))]
    (put! ch msg (fn [_] (close! ch)))))

(defn run-example
  "call (start-langohr) and (start-listening) before this.
   after that, you may call (run-example) or (send-message) any number of times.
   call (stop-langohr) to cleanup the used resources."
  []
  (send-message "Hello!")
  (Thread/sleep 500)
  (send-message "This is a message.")
  (Thread/sleep 500)
  (send-message "Goodbye."))
