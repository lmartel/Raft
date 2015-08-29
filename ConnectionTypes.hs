{-# LANGUAGE MultiParamTypeClasses #-}
module ConnectionTypes where
import Control.Lens
import Data.IORef
import Data.Aeson

import RaftTypes
import MessageTypes
import Config

class Connection c where
  request :: Message -> c -> IO (Maybe Message)
  respond :: Message -> c -> IO ()

  listenMaybe :: c -> IO (Maybe Message)
  listen :: c -> IO Message
  listen conn = listenMaybe conn >>= (\mMsg -> case mMsg of
                                                Nothing -> listen conn
                                                (Just msg) -> return msg
                                     )

  fromConfig :: CohortConfig -> IO c

data NilConnection = NilConnection

instance Connection NilConnection where
  request _ _ = return Nothing
  respond _ _ = return ()

  listenMaybe _ = return Nothing
  listen = error "NilConnection.listen :: listening on a NilConnection will hang"
  fromConfig _ = return NilConnection

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

  fromConfig _ = return FakeConnection

-- TODO get updates from the client somehow
class ClientConnection c a where
  getUpdate :: c a -> IO a
