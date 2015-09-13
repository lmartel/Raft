{-# LANGUAGE OverloadedStrings #-}
module Config where
import Data.Aeson
import Control.Monad

import RaftTypes

kReconnect :: Int
-- kReconnect = 200
kReconnect = 1500

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
  toJSON (ClusterConfig ldr cs) = object [
    "leader" .= ldr
    , "servers" .= cs
    ]

instance FromJSON ClusterConfig where
  parseJSON (Object v) = ClusterConfig
                         <$> v .: "leader"
                         <*> v .: "servers"
