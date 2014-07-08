(ns user
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [com.stuartsierra.component :as component]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :refer [chan put! <! <!! go close!]]))

(def default-exchange-name "")
(def qname "langohr.examples.hello-world")

(defn r-chan [langohr-conn]
  (let [rch (lch/open (:langohr-conn langohr-conn))]
    (lq/declare rch qname :exclusive false :auto-delete true)
    rch))

(defn take-chan [langohr-chan]
  (let [ch (chan 1)
        rch (:langohr-chan langohr-chan)]
    (assert rch)
    (lq/declare rch qname :exclusive false :auto-delete true)
    (lc/subscribe rch qname
                  (fn [_ {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
                    (put! ch (String. payload "UTF-8"))) :auto-ack true)
    ch))

(defn put-chan [langohr-chan]
  (let [ch (chan 1)
        rch (:langohr-chan langohr-chan)]
    (assert rch)
    (lq/declare rch qname :exclusive false :auto-delete true)
    (go (loop []
          (let [msg (<! ch)]
            (when-not (nil? msg)
              (lb/publish rch default-exchange-name qname msg :content-type "text/plain" :type "greetings.hi")
              (recur)))))
    ch))

(defrecord LangohrConn []
  component/Lifecycle
  (start [this]
    (assoc this :langohr-conn (rmq/connect)))
  (stop [this]
    (rmq/close (:langohr-conn this))
    (assoc this :langohr-conn nil)))

(defrecord LangohrChan [langohr-conn]
  component/Lifecycle
  (start [this]
    (assoc this :langohr-chan (r-chan langohr-conn)))
  (stop [this]
    (rmq/close (:langohr-chan this))
    (assoc this
      :langohr-chan nil
      :langohr-conn nil)))

(defrecord AsyncChannels [langohr-chan]
  component/Lifecycle
  (start [this]
    (assoc this
      :take-chan (take-chan langohr-chan)
      :put-chan (put-chan langohr-chan)))
  (stop [this]
    (close! (:take-chan this))
    (close! (:put-chan this))
    (assoc this
      :langohr-chan nil
      :take-chan nil
      :put-chan nil)))

(defn q-system []
  (component/system-map
   :langohr-conn (map->LangohrConn {})
   :langohr-chan (component/using
                  (map->LangohrChan {})
                  [:langohr-conn])
   :async-channels (component/using
                    (map->AsyncChannels {})
                    [:langohr-chan])))

(def system (q-system))

(defn start [] (alter-var-root #'system component/start))
(defn stop [] (alter-var-root #'system component/stop))

(defn putc [] (get-in system [:async-channels :put-chan]))
(defn takec [] (get-in system [:async-channels :take-chan]))


(defn run-example
  "call after (start)"
  []
  (go (println "Got msg: " (<! (takec))))
  (put! (putc) "Hello!"))

