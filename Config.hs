{-# LANGUAGE DeriveGeneric #-}
module Config where
import GHC.Generics
import Data.Aeson

import RaftTypes

data CohortConfig = CohortConfig {
  id :: ServerId,
  hostname :: Hostname,
  port :: Port
  } deriving (Show, Generic)

data ClusterConfig = ClusterConfig {
  leader :: ServerId,
  servers :: [CohortConfig]
  } deriving (Show, Generic)

instance ToJSON CohortConfig
instance FromJSON CohortConfig
instance ToJSON ClusterConfig
instance FromJSON ClusterConfig
