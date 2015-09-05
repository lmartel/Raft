{-# LANGUAGE MultiParamTypeClasses #-}
module ConnectionTypes where
import Control.Lens
import Control.Monad
import Data.IORef
import Data.Aeson

import Network

import RaftTypes
import MessageTypes
import Config

class Connection c where
  request :: Message -> c -> IO (Maybe Message)
  respond :: Message -> c -> IO ()
  listen :: c -> IO (Maybe Message)

  fromConfig :: CohortConfig -> IO c

data NilConnection = NilConnection
                   deriving Show

instance Connection NilConnection where
  request _ _ = return Nothing
  respond _ _ = return ()

  listen _ = return Nothing
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

  listen FakePartition = listen FakePartition
  listen FakeConnection = return . Just . appendEntries 20 1 0 0 newEntries 3 $ me
    where newEntries = zip [1..] [LogEntry 19 "first log entry", LogEntry 20 "second log entry"]
          me = MessageInfo 1 1337

  fromConfig _ = return FakeConnection

data NetworkConnection = NetworkConnection ServerId HostName PortID
                       deriving Show

networkSend :: Message -> NetworkConnection -> IO ()
networkSend msg (NetworkConnection _ host portNum) = Network.sendTo host portNum . show . encode $ msg

networkRecv :: NetworkConnection -> IO (Maybe Message)
networkRecv (NetworkConnection _ host portNum) = liftM (decode . read) $ Network.recvFrom host portNum

instance Connection NetworkConnection where
  fromConfig (CohortConfig sid host portNum) = pure $ NetworkConnection sid host (PortNumber . fromIntegral $ portNum)

  request msg net = networkSend msg net >> networkRecv net
  respond = networkSend
  listen = networkRecv

-- TODO get updates from the client somehow
class ClientConnection c a where
  getUpdate :: c a -> IO a
