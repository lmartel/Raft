{-# LANGUAGE OverloadedStrings #-}
module Config where
import Data.Aeson
import Control.Monad

import RaftTypes

type Microsec = Int

kReconnect :: Microsec
-- kReconnect = 200000
kReconnect = 1500000

kGenerateClientUpdates :: Microsec
kGenerateClientUpdates = 700000
-- kGenerateClientUpdates = 4000000

kCommitReport :: Microsec
kCommitReport = 1500000

kHeartbeat :: Microsec
kHeartbeat = 300000

kTimeoutMin :: Microsec
kTimeoutMin = 300000

kTimeoutMax :: Microsec
kTimeoutMax = 500000

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
