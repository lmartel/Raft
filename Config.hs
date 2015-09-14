{-# LANGUAGE OverloadedStrings #-}
module Config where
import Data.Aeson
import Control.Monad

import RaftTypes

kReconnect :: Int
-- kReconnect = 200
kReconnect = 1500

kCommitReport :: Int
kCommitReport = 1500

kTimeoutMin :: Int
kTimeoutMin = 300

kTimeoutMax :: Int
kTimeoutMax = 500

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
