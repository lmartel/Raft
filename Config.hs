{-# LANGUAGE OverloadedStrings #-}
module Config where
import Control.Monad
import Data.Aeson

import RaftTypes

type Millisec = Float
type Microsec = Int

realtimeRatio :: Float
realtimeRatio = 1 / 20 -- "Slow-mo"
-- realtimeRatio = 1       -- Production speed

milli :: Millisec -> Microsec
milli = round . (/ realtimeRatio) . (* 1000)

kReconnect :: Microsec
kReconnect = milli 100

kGenerateClientUpdates :: Microsec
kGenerateClientUpdates = milli 700

kCommitReport :: Microsec
kCommitReport = milli 1000

kHeartbeat :: Microsec
kHeartbeat = milli 100

kTimeoutMin :: Microsec
kTimeoutMin = milli 300

kTimeoutMax :: Microsec
kTimeoutMax = milli 500

instance ToJSON CohortConfig where
  toJSON (CohortConfig sid host port) = object [
    "id" .= sid
    , "hostname" .= host
    , "port" .= port
    ]

instance FromJSON CohortConfig where
  parseJSON (Object v) = CohortConfig
                         <$> v .: "id"
                         <*> v .: "hostname"
                         <*> v .: "port"
  parseJSON _ = mzero

instance ToJSON ClusterConfig where
  toJSON (ClusterConfig ldr cl cs) = object [
    "leader" .= ldr
    , "client" .= cl
    , "servers" .= cs
    ]

instance FromJSON ClusterConfig where
  parseJSON (Object v) = ClusterConfig
                         <$> v .: "leader"
                         <*> v .: "client"
                         <*> v .: "servers"
  parseJSON _ = mzero

instance ToJSON ClientConfig where
  toJSON (ClientConfig host port) = object [
    "hostname" .= host
    , "port" .= port
    ]

instance FromJSON ClientConfig where
  parseJSON (Object v) = ClientConfig
                         <$> v .: "hostname"
                         <*> v .: "port"
  parseJSON _ = mzero
