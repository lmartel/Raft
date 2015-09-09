{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
module ConnectionTypes where
import Control.Concurrent
import Control.Lens
import Control.Monad
import Data.IORef
import Data.Aeson

import System.Posix.Signals
import System.IO
import Network
import Network.Socket


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

  listen FakePartition = ConnectionTypes.listen FakePartition
  listen FakeConnection = return . Just . appendEntries 20 1 0 0 newEntries 3 $ me
    where newEntries = zip [1..] [LogEntry 19 "first log entry", LogEntry 20 "second log entry"]
          me = MessageInfo 1 1337

  fromConfig _ = return FakeConnection

data SimpleNetworkConnection = SimpleNetworkConnection ServerId HostName PortID
                             deriving Show

instance Connection SimpleNetworkConnection where
  fromConfig (CohortConfig sid host portNum) = pure $ SimpleNetworkConnection sid host (PortNumber . fromIntegral $ portNum)

  request msg net = respond msg net >> ConnectionTypes.listen net
  respond msg (SimpleNetworkConnection _ host portNum) = Network.sendTo host portNum . show . encode $ msg
  listen (SimpleNetworkConnection _ host portNum) = liftM (decode . read) $ Network.recvFrom host portNum

data HandleConnection = HandleConnection ServerId Handle
                      deriving Show

instance Connection HandleConnection where
  fromConfig (CohortConfig sid host portNum) = do
    putStrLn ("Connecting to follower at " ++ host ++ ":" ++ show portNum)
    hdl <- connectTo host . PortNumber . fromIntegral $ portNum
    return . HandleConnection sid $ hdl

  request msg net = respond msg net >> ConnectionTypes.listen net
  respond msg (HandleConnection _ hdl) = hPrint hdl (encode msg)
  listen (HandleConnection _ hdl) = decode . read <$> hGetLine hdl



class ClientConnection c where
  getLogEntry :: c a -> IO a
  committed :: Show a => a -> c a -> IO ()

data SimpleIncrementingClient a = SIClient (Int -> a) (IORef Int)

newTestSIClient :: IO (SimpleIncrementingClient String)
newTestSIClient = SIClient (\i -> "Log Entry: spacedate " ++ show i) <$> newIORef 1

instance ClientConnection SimpleIncrementingClient where
  getLogEntry (SIClient convert ctr) = threadDelay 333000 >> convert <$> atomicModifyIORef ctr (\i -> (i + 1, i))
  committed x _ = putStrLn $ "!! COMMITTED `" ++ show x ++ "`"


data AbortClient a = AbortClient

instance ClientConnection AbortClient where
  getLogEntry _ = raiseSignal keyboardSignal >> threadDelay 999999 >> error "unused"
  committed x _ = putStrLn $ "!! COMMITTED `" ++ show x ++ "`"
