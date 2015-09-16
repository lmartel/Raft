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
import System.IO.Error
import Network
import Network.Socket
import Control.Exception
import GHC.IO.Exception

import RaftTypes
import MessageTypes
import Config
import Debug


class Connection c where
  respond :: Message -> c -> IO ()
  listen :: c -> IO (Maybe Message)

  request :: Message -> c -> IO (Maybe Message)
  request msg net = respond msg net >> ConnectionTypes.listen net

  fromConfig :: CohortConfig -> IO c

data NilConnection = NilConnection
                   deriving Show

instance Connection NilConnection where
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


-- Not used, because Network.recvFrom is hard to reuse reliably
data SimpleNetworkConnection = SimpleNetworkConnection ServerId HostName PortID
                             deriving Show

instance Connection SimpleNetworkConnection where
  fromConfig (CohortConfig sid host portNum) = pure $ SimpleNetworkConnection sid host (PortNumber . fromIntegral $ portNum)

  respond msg (SimpleNetworkConnection _ host portNum) = Network.sendTo host portNum . show . encode $ msg
  listen (SimpleNetworkConnection _ host portNum) = liftM (decode . read) $ Network.recvFrom host portNum

-- Simple connection wrapper for a Handle created from a Socket.
-- Used by Followers to process requests.
data SimpleHandleConnection = SimpleHandleConnection Handle
                            deriving Show

instance Connection SimpleHandleConnection where
  fromConfig = undefined

  respond msg (SimpleHandleConnection hdl) = respondMaybe' msg (Just hdl) `catch` exitOnError
  listen (SimpleHandleConnection hdl) = listenMaybe' (Just hdl) `catch` (\ex -> exitOnError ex >> return Nothing)


exitOnError :: IOError -> IO ()
exitOnError ex = debug ("SimpleHandleConnection :: " ++ show ex) $ myThreadId >>= killThread

-- Handle with reconnection capability.
-- Used by Leaders to send requests to Followers.
data HandleConnection = HandleConnection CohortConfig (IORef (Maybe Handle))

instance Show HandleConnection where
  show (HandleConnection conf _) = "HandleConnection " ++ show conf


debugConnectError :: Bool -> CohortConfig -> IOError -> IO ()
debugConnectError verbose conf ex
  -- Connection refused.
  | isDoesNotExistError ex                 = when verbose $ putStrLn ("Cannot connect to " ++ followerInfo)
  | isEOFError ex                          = when verbose $ putStrLn (followerInfo ++ " finished.")
  | ioeGetErrorType ex == ResourceVanished = when verbose $ putStrLn (followerInfo ++ " disconnected.")
  | otherwise                              = error ("Unknown connection error: " ++ show ex)
  where followerInfo = "Follower " ++ show (view cohortId conf)
                       ++ " (" ++ view cohortHostname conf
                       ++ ":" ++ show (view cohortPort conf)
                       ++ ")"

connectHandle :: Bool -> CohortConfig -> IO (Maybe Handle)
connectHandle verbose conf@(CohortConfig _ host portNum) =
  catch (do
    hdl <- connectHandle'
    tid <- myThreadId
    when verbose $ putStrLn ("Connected to follower at " ++ host ++ ":" ++ show portNum ++ "." ++ "[" ++ show tid ++ "]")
    return (Just hdl)
  ) (\ex -> debugConnectError verbose conf ex >> return Nothing)
  where connectHandle' :: IO Handle
        connectHandle' = connectTo host . PortNumber . fromIntegral $ portNum

connectMaybe :: Bool -> HandleConnection -> IO ()
connectMaybe verbose (HandleConnection conf handleRef) = do
  mHdl <- readIORef handleRef
  hdl' <- case mHdl of
             Nothing -> connectHandle verbose conf
             (Just hdl) -> return . Just $ hdl
  writeIORef handleRef hdl'

-- Try to send.
-- If exception: throw away connection. Do not (immediately) reconnect.
respondMaybe :: Message -> HandleConnection -> IO ()
respondMaybe msg conn@(HandleConnection conf hdlRef) = do
  mHdl <- readIORef hdlRef
  respondMaybe' msg mHdl `catch` (\ex -> debugConnectError False conf ex >> writeIORef hdlRef Nothing)

respondMaybe' :: Message -> Maybe Handle -> IO ()
respondMaybe' _ Nothing = return ()
respondMaybe' msg (Just hdl') = hPrint hdl' (encode msg)

listenMaybe :: HandleConnection -> IO (Maybe Message)
listenMaybe conn@(HandleConnection conf hdlRef) = do
  mHdl <- readIORef hdlRef
  listenMaybe' mHdl `catch` (\ex -> debugConnectError True conf ex >> writeIORef hdlRef Nothing >> return Nothing)

listenMaybe' :: Maybe Handle -> IO (Maybe Message)
listenMaybe' Nothing = return Nothing
listenMaybe' (Just hdl) = decode . read <$> hGetLine hdl

instance Connection HandleConnection where
  fromConfig conf = HandleConnection conf <$> newIORef Nothing

  respond msg conn = connectMaybe False conn >> respondMaybe msg conn
  listen conn = connectMaybe True conn >> listenMaybe conn

  -- override `request` : should only try to connect once. If request fails, shouldn't listen for response.
  request msg conn = connectMaybe True conn >> respondMaybe msg conn >> listenMaybe conn



class ClientConnection c a where
  getLogEntry :: c a -> IO a
  committed :: Show a => a -> c a -> IO ()

  fromClientConfig :: ClientConfig -> IO (c a)

data SimpleIncrementingClient a = SIClient (Int -> a) (IORef Int)

newTestSIClient :: IO (SimpleIncrementingClient String)
newTestSIClient = SIClient (\i -> "Log Entry: spacedate " ++ show i) <$> newIORef 1

instance ClientConnection SimpleIncrementingClient String where
  getLogEntry (SIClient convert ctr) = threadDelay kGenerateClientUpdates >> convert <$> atomicModifyIORef ctr (\i -> (i + 1, i))
  committed x _ = putStrLn ("!! COMMITTED `" ++ show x ++ "`")

  fromClientConfig _ = newTestSIClient


data AbortClient a = AbortClient

instance ClientConnection AbortClient a where
  getLogEntry _ = raiseSignal keyboardSignal >> threadDelay 999999 >> error "unused"
  committed _ _ = debug "ERROR! AbortClient should not receive commits." return ()

  fromClientConfig _ = return AbortClient
