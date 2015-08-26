{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
module ConnectionTypes where
import Control.Lens
import GHC.Generics
import Data.Aeson

import RaftTypes
import MessageTypes

type Hostname = String
type Port = Int

data ClusterConfig = ClusterConfig {
  _leader :: ServerId,
  _servers :: [(ServerId, Hostname, Port)]
  } deriving (Show, Generic)
makeLenses ''ClusterConfig
instance ToJSON ClusterConfig
instance FromJSON ClusterConfig

class Connection c where
  request :: Message -> c -> IO (Maybe Message)
  respond :: Message -> c -> IO ()

  listenMaybe :: c -> IO (Maybe Message)
  listen :: c -> IO Message
  listen conn = listenMaybe conn >>= (\mMsg -> case mMsg of
                                                Nothing -> listen conn
                                                (Just msg) -> return msg
                                     )

  fromHostPort :: Hostname -> Port -> c

data FakeConnection = FakeConnection | FakePartition
                    deriving Show

instance Connection FakeConnection where
  request msg FakeConnection = return . Just . (case view msgType msg of
                                     AppendEntries -> appendEntriesResponse 7 True
                                     RequestVote -> requestVoteResponse 9 False
                                     ) $ view info msg
  request _ FakePartition = return Nothing
  respond _ _ = return ()

  listenMaybe FakePartition = listenMaybe FakePartition
  listenMaybe FakeConnection = return . Just . appendEntries 20 1 0 0 newEntries 3 $ me
    where newEntries = zip [1..] [LogEntry 19 "first log entry", LogEntry 20 "second log entry"]
          me = MessageInfo 1 1337

  fromHostPort _ _ = FakeConnection

-- data SelfConnection = SelfConnection (Message -> Message)

-- instance Connection SelfConnection where
--   request msg (SelfConnection handler) = return . Just . handler $ msg
--   respond _ _ = return ()


data SelfConnection s c a = SelfConnection (Server s c a)

instance Connection (SelfConnection s c a) where
  request msg (SelfConnection s2) =
