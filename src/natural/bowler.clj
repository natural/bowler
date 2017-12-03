(ns natural.bowler
  (:require [natural.bowler.core :reload true]))


(seq
 (map (fn [[a b c]] (intern a b c))
      [['natural.bowler 'resource-collection natural.bowler.core/resource-collection]
       ['natural.bowler 'resource-detail     natural.bowler.core/resource-detail]
       ['natural.bowler 'signal-context  natural.bowler.core/signal-context]
       ['natural.bowler 'api-handler  natural.bowler.core/api-handler]]))

