{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ExistentialQuantification #-}
module Raft where
import qualified Prelude (log)
import Prelude hiding (log)
import System.Environment (getArgs)
import Control.Concurrent
import Control.Lens
import Control.Monad
import Control.Exception.Base
import Data.Aeson
import Data.List
import Data.IORef
import Data.Maybe
import Data.Either
import Control.Monad.State
import System.IO.Unsafe
import Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as ByteString
import System.Posix.Signals
import System.IO
import Network.Socket hiding (listen)
import qualified Network.Socket as Sock (listen)


import Test.HUnit

import RaftTypes
import MessageTypes
import ConnectionTypes
import JsonStorage
import Config

--- Initialization

initializeFollower :: ServerConfig s c a -> Server s c a
initializeFollower conf = Server {
  _currentTerm = Term 0,
  _votedFor = Nothing,
  _log = Log [],
  _commitIndex = LogIndex 0,
  _lastApplied = LogIndex 0,
  _nextIndex = Nothing,
  _matchIndex = Nothing,

  _config = set role Follower conf,
  _outstanding = Map.empty
  }

promoteToLeader :: Server s c a -> Server s c a
promoteToLeader base = set (config.role) Leader . set nextIndex (Just nis) . set matchIndex (Just mis) $ base
  where nis :: ServerMap LogIndex
        nis = Map.map (\_ -> viewLastLogIndex base) . view (config.cohorts) $ base
        mis :: ServerMap LogIndex
        mis = Map.map (\_ -> LogIndex 0) . view (config.cohorts) $ base

demoteToFollower :: Server s c a -> Server s c a
demoteToFollower = set (config.role) Follower . set nextIndex Nothing . set matchIndex Nothing

--- Sending/receiving messages

atomicGetNextMessageId :: IORef MessageId -> IO MessageId
atomicGetNextMessageId ref = atomicModifyIORef ref (\mid -> (mid + 1, mid))

atomicGetNextMessageIds :: Int -> IORef MessageId -> IO [MessageId]
atomicGetNextMessageIds n ref = atomicModifyIORef ref (\mid -> (mid + fromIntegral n, take n [mid..]))

requestInfo :: IORef MessageId -> Server s c a -> IO MessageInfo
requestInfo midRef serv = MessageInfo (view serverId serv) <$> atomicGetNextMessageId midRef

responseInfo :: Message -> Server s c a -> MessageInfo
responseInfo req serv = MessageInfo (view serverId serv) (view (info.msgId) req)

prepareBroadcast :: IORef MessageId -> BaseMessage -> Server s c a -> IO [PendingMessage c]
prepareBroadcast midRef msg serv
  | view (config.role) serv /= Leader = return []
  | otherwise = requests <$> atomicGetNextMessageIds (length . serverCohorts $ serv) midRef
  where requests = zipWith prepareMessage (serverCohorts serv)
        prepareMessage c mid = (c, msg $ MessageInfo (view serverId serv) mid)

expectResponsesTo :: [PendingMessage c] -> Raft s c a ()
expectResponsesTo msgs = get >>= put . over outstanding multiInsert
  where multiInsert msgMap = foldl singleInsert msgMap msgs
        singleInsert :: Map MessageId (PendingMessage c) -> PendingMessage c -> Map MessageId (PendingMessage c)
        singleInsert msgMap pendMsg = Map.insertWithKey dupeInsert (view (info.msgId) $ snd pendMsg) pendMsg msgMap
        dupeInsert k _ _ = error $ "expectResponsesTo :: second message sent with id " ++ show k

retryRequest :: Connection c => MessageId -> Server s c a -> IO (Maybe Message)
retryRequest mid serv = case Map.lookup mid (view outstanding serv) of
                         Nothing -> debug ("retryRequest :: messageId " ++ show mid ++ " does not exist.") (return Nothing)
                         (Just pendMsg) -> sendRequest pendMsg

sendRequest :: Connection c => PendingMessage c -> IO (Maybe Message)
sendRequest (c,m) = request m c

broadcast :: Connection c => [PendingMessage c] -> IO [Maybe Message]
broadcast = mapM sendRequest

broadcastUntil :: Connection c => ([Message] -> Bool) -> [PendingMessage c] -> IO [Message]
broadcastUntil fn = broadcastUntil' . map Right
  where broadcastUntil' :: Connection c => [Either Message (PendingMessage c)] -> IO [Message]
        broadcastUntil' reqs = broadcast' reqs >>= retryIfNeeded (rights reqs)
        broadcast' :: Connection c => [Either Message (PendingMessage c)] -> IO [Maybe Message]
        broadcast' = mapM (\e -> case e of
                                  (Left msg) -> return . Just $ msg
                                  (Right req) -> sendRequest req
                          )

        retryIfNeeded :: Connection c => [PendingMessage c] -> [Maybe Message] -> IO [Message]
        retryIfNeeded reqs responses = if fn $ catMaybes responses
                                  then return $ catMaybes responses
                                  else broadcastUntil' $ zipWith retrySome reqs responses
        retrySome :: Connection c => PendingMessage c -> Maybe Message -> Either Message (PendingMessage c)
        retrySome req Nothing = Right req
        retrySome _ (Just res) = Left res

--- Raft algorithm core

handleRequest :: FromJSON a => Message -> Raft s c a Message
handleRequest msg = case view msgType msg of
  AppendEntries -> processAppendEntries msg
  RequestVote -> undefined

handleResponse :: FromJSON a => Message -> Raft s c a ()
handleResponse msg = do
  let maybeMalformed = [
        liftM checkOutOfDate (term msg),
        handleResponse'
        ]
  case sequence maybeMalformed of
   Nothing -> get >>= put
   (Just states) -> sequence_ states
  where checkOutOfDate :: Term -> Raft s c a ()
        checkOutOfDate followerTerm = do
             me <- get
             if view currentTerm me < followerTerm
               then put . set currentTerm followerTerm . demoteToFollower $ me
               else put me

        handleResponse' :: Maybe (Raft s c a ())
        handleResponse' = case view msgType msg of
                           AppendEntriesResponse -> liftM (handleAppendEntriesResponse msg) $ success msg
                           RequestVoteResponse -> liftM (handleRequestVoteResponse msg) $ voteGranted msg

-- On failure: decrement nextIndex, retry later
-- On success: update matchIndex and nextIndex, then resolve (delete) the pending message.
handleAppendEntriesResponse :: Message -> Bool -> Raft s c a ()
handleAppendEntriesResponse msg False = do
  me <- get
  put $ over nextIndex (fmap $ Map.insertWith (\_ old -> old - 1) (view (info.msgFrom) msg) (1 + view commitIndex me)) me


handleAppendEntriesResponse msg True = do
  me <- get
  case Map.lookup respId $ view outstanding me of
   Nothing -> debug ("Got response to message (ID: "
                     ++ show respId
                     ++ ") never sent or already resolved. Ignoring.")
              (put me)
   (Just (_, req)) -> case lastSentIndex =<< entries req of
                       Nothing -> put me
                       (Just matchedId) -> put
                                           . checkCommitted (sentIndices =<< entries req)
                                           . over matchIndex (fmap $ Map.insert responder matchedId)
                                           . over nextIndex (fmap . Map.insert responder $ matchedId + 1)
                                           $ me
  where respId = view (info.msgId) msg
        responder = view (info.msgFrom) msg

        sentIndices :: [(LogIndex, LogEntry NilEntry)] -> Maybe [LogIndex]
        sentIndices [] = Nothing
        sentIndices es = Just . map fst $ es

        lastSentIndex :: [(LogIndex, LogEntry NilEntry)] -> Maybe LogIndex
        lastSentIndex = fmap last . sentIndices

checkCommitted :: Maybe [LogIndex] -> Server s c a -> Server s c a
checkCommitted Nothing = id
checkCommitted (Just x) = checkCommitted' x
  where checkCommitted' [] serv = serv
        checkCommitted' (n:ns) serv = if n > view commitIndex serv
                                         && majorityMatched n serv
                                         && termAtIndex n serv == Just (view currentTerm serv)
                                      then checkCommitted' ns . set commitIndex n $ serv
                                      else serv


majorityMatched :: LogIndex -> Server s c a -> Bool
majorityMatched n serv = case view matchIndex serv of
  Nothing -> False
  (Just mp) -> 2 * Map.size (Map.filter (>= n) mp)
               > Map.size mp

handleRequestVoteResponse :: Message -> Bool -> Raft s c a ()
handleRequestVoteResponse = undefined

processAppendEntries :: FromJSON a => Message -> Raft s c a Message
processAppendEntries msg = do
  me <- get
  case validateAppendEntries me msg of
   Nothing -> do put me
                 return (response False me)
   (Just entrs) -> let me' = set currentTerm (fromJust $ term msg)
                             . over log (updateLogWith entrs)
                             . set commitIndex (updateCommitIndex me $ leaderCommit msg)
                             $ me
                   in do put me'
                         return (response True me')
  where updateCommitIndex :: Server s c a -> Maybe LogIndex -> LogIndex
        updateCommitIndex me Nothing = view commitIndex me
        updateCommitIndex me (Just theirs) = min theirs (viewLastLogIndex me)

        response :: Bool -> Server s c a -> Message
        response b me = appendEntriesResponse (view currentTerm me) b $ responseInfo msg me

        updateLogWith :: [IndexedEntry a] -> Log a -> Log a
        updateLogWith es = appendNew es . deleteConflicted es

        deleteConflicted :: [IndexedEntry a] -> Log a -> Log a
        deleteConflicted es lg = case map fst . filter (isConflict lg) $ es of
                                  [] -> lg
                                  (i:is) -> Log . map snd . takeWhile ((< foldl min i is) . fst) . withIndices $ lg

        isConflict :: Log a -> IndexedEntry a -> Bool
        isConflict lg (i, e) = case entry i lg >>= Just . (/= view entryTerm e) . view entryTerm of
                                 (Just b) -> b
                                 Nothing -> False

        appendNew :: [IndexedEntry a] -> Log a -> Log a
        appendNew [] lg = lg
        appendNew ((i,e):es) lg
          | i == 1 + lastIndex lg = appendNew es $ over logEntries (++ [e]) lg
          | otherwise = debug "processAppendEntries :: Error, there's a hole in the log!" lg

validateAppendEntries :: FromJSON a => Server s c a -> Message -> Maybe [IndexedEntry a]
validateAppendEntries serv msg =  case Just . all (== True) =<< sequence [
  term msg >>= Just . (>= view currentTerm serv),
  noPrevLogEntries `maybeOr` prevLogEntryMatches
  ] of
                                  (Just True) -> entries msg
                                  _ -> Nothing
  where noPrevLogEntries :: Maybe Bool
        noPrevLogEntries = prevLogIndex msg >>= Just . (== 0)
        prevLogEntryMatches :: Maybe Bool
        prevLogEntryMatches = liftM2 (==) (prevLogTerm msg) $ prevLogIndex msg >>= flip entry (view log serv) >>= Just . view entryTerm
        maybeOr :: Maybe Bool -> Maybe Bool -> Maybe Bool
        maybeOr (Just True) _ = Just True
        maybeOr _ b2 = b2

--- Startup and main

kConfigDir :: String
kConfigDir = "conf/"
kConfigFile :: ServerId -> String
kConfigFile (ServerId sid) = kConfigDir ++ "config." ++ show sid ++ ".json"

kLogDir :: String
kLogDir = "db/"
kLogFile :: ServerId -> String
kLogFile (ServerId sid) = kLogDir ++ "log." ++ show sid ++ ".json"

configureCohorts :: Connection c => ClusterConfig -> IO (ServerMap c)
configureCohorts conf = liftM Map.fromList $ pure conf
                        >>= connectAll . map (\conf@(CohortConfig sid _ _) -> (sid, fromConfig conf)) . view clusterServers
                   where connectAll :: [(a, IO b)] -> IO [(a, b)]
                         connectAll connections = return . zip (map fst connections) =<< mapM snd connections

configureSelf :: Connection c => CohortConfig -> ClusterConfig -> s a -> IO (ServerConfig s c a)
configureSelf myConf cluster stor = liftM (\clust' -> ServerConfig myRole myConf clust' stor) (configureCohorts cluster)
  where myRole = if view cohortId myConf == view clusterLeader cluster
                 then Leader
                 else Follower

readJSONConfig :: (Persist s, Connection c) => String -> ServerId -> IO (ServerConfig s c String)
readJSONConfig f myId = do
  confStr <- ByteString.readFile f
  let maybeConf = decode confStr >>= (\clust -> liftM2 (,) (Just clust) (myConf clust))
  case maybeConf of
   Nothing -> return . error $ "cannot read or parse config file: " ++ f
   (Just (clust, me)) -> configureSelf me clust (fromName . kLogFile $ view cohortId me)
  where myConf :: ClusterConfig -> Maybe CohortConfig
        myConf = find (\someConf -> view cohortId someConf == myId) . view clusterServers

        filterMe :: ServerId -> ClusterConfig -> ClusterConfig
        filterMe sid = over clusterServers (filter ((/= sid) . view cohortId))

debugMain :: (Show c, Show a) => (Server s c a -> IO (Server s c a)) -> Server s c a -> IO (Server s c a)
debugMain mainFn serv = do
  let servInfo = show (view serverId serv) ++ " " ++ show (view (config.role) serv)
  putStrLn $ "===== " ++ servInfo ++ " STARTING ====="
  print $ view config serv
  print serv

  serv' <- mainFn serv

  putStrLn $ "===== " ++ servInfo ++ " FINISHING ====="
  print serv'
  putStrLn "===== ALL DONE ====="

  return serv'

followerMain :: (Persist s, Connection c, FromJSON a, ToJSON a, Show a) => Server s c a -> IO (Server s c a)
followerMain serv0 = do
  serverState <- newMVar serv0
  workerThreads <- newMVar []
  allDone <- newEmptyMVar
  mainThread <- myThreadId

  -- create socket
  sock <- socket AF_INET Stream 0
  -- make socket immediately reusable - eases debugging.
  setSocketOption sock ReuseAddr 1
  -- listen on TCP port
  bindSocket sock (SockAddrInet (fromIntegral . view (config.ownCohort.cohortPort) $ serv0) iNADDR_ANY)
  -- allow a maximum of (#COHORTS) outstanding connections
  Sock.listen sock 5

  installHandler keyboardSignal (Catch $ cleanupAndExit serverState workerThreads allDone) Nothing
  snocQueue workerThreads <$> forkIO (spawnLoop sock serverState workerThreads)
  takeMVar allDone
  takeMVar serverState

  where spawnLoop :: (Persist s, FromJSON a, ToJSON a) => Socket -> MVar (Server s c a) -> MVar [ThreadId] -> IO ()
        spawnLoop sock servState listenersQueue = do
          (sock', _) <- accept sock
          hdl <- debug "Worker thread listening..." $ socketToHandle sock' ReadWriteMode
          hSetBuffering hdl NoBuffering

          newListener <- forkIO . forever $ listenAndRespond servState (HandleConnection hdl)
          snocQueue listenersQueue newListener

          spawnLoop sock servState listenersQueue

        cleanupAndExit :: (Show a) => MVar (Server s c a) -> MVar [ThreadId] -> MVar () -> IO ()
        cleanupAndExit serverState workersQueue done = do
          serverLock <- debug "Cleaning up..." $ takeMVar serverState
          mapM killThread <$> takeMVar workersQueue
          putMVar serverState serverLock
          debug "Workers killed." $ putMVar done ()


snocQueue :: MVar [a] -> a -> IO ()
snocQueue q v = takeMVar q >>= putMVar q . (v :)

listenAndRespond :: (Persist s, Connection conn, FromJSON a, ToJSON a) => MVar (Server s c a) -> conn -> IO ()
listenAndRespond servBox conn = do
  maybeReq <-  listen conn
  case debug "Worker thread received request." maybeReq of
   Nothing -> return ()
   (Just req) -> do
     serv <- takeMVar servBox
     let (resp, serv') = runState (handleRequest req) serv
     persist serv'
     putMVar servBox serv'
     respond resp conn

leaderMain :: (ClientConnection cli, Connection c) => cli String -> Server s c String -> IO (Server s c String)
leaderMain cli serv0 = do
  serverState <- newEmptyMVar
  nextMessageId <- newIORef 0
  allDone <- newEmptyMVar

  let nCohorts = Map.size . view (config.cohorts) $ serv0

  broadcastTid <- forkIO $ broadcastThread cli nextMessageId serverState
  installHandler keyboardSignal (Catch $ cleanupAndExit serverState broadcastTid allDone) Nothing
  putMVar serverState serv0
  takeMVar allDone
  takeMVar serverState
    where cleanupAndExit :: MVar (Server s c a) -> ThreadId -> MVar () -> IO ()
          cleanupAndExit servBox broadcastTid done = do
            serverLock <- takeMVar servBox
            killThread broadcastTid
            putMVar servBox serverLock
            putMVar done ()

broadcastThread :: (ClientConnection cli, Connection c) => cli String -> IORef MessageId -> MVar (Server s c String) -> IO ()
broadcastThread cli nextMessageId servBox = do
  serv0 <- takeMVar servBox
  let catchUp = appendEntriesFromLeader serv0 . logWithIndices $ serv0
  serv <- leaderBroadcast nextMessageId catchUp serv0
  putMVar servBox serv
  leaderLoop nextMessageId cli servBox

leaderLoop :: (ClientConnection cli, Connection c) => IORef MessageId -> cli String -> MVar (Server s c String) -> IO ()
leaderLoop nextMid cli servBox = do
  nextLogEntry <- getLogEntry cli
  serv0 <- takeMVar servBox

  let serv = over log (logMap (++ [LogEntry (view currentTerm serv0) nextLogEntry])) serv0
  let update = appendEntriesFromLeader serv [last . logWithIndices $ serv]
  requests <- prepareBroadcast nextMid update serv
  responses <- broadcastUntil majoritySuccessful requests
  let serv' = execState (expectResponsesTo requests >> mapM handleResponse responses) serv
  putMVar servBox serv'

  let newCommits = mapMaybe (flip entry $ view log serv) [1 + view commitIndex serv .. view commitIndex serv']
  mapM_ (flip committed cli . view entryData) newCommits
  leaderLoop nextMid cli servBox

leaderBroadcast :: (Connection c, ToJSON a, FromJSON a) => IORef MessageId -> BaseMessage -> Server s c a -> IO (Server s c a)
leaderBroadcast nextMessageId req serv = do
  requests <- prepareBroadcast nextMessageId req serv
  responses <- broadcastUntil majoritySuccessful requests
  return $ execState (expectResponsesTo requests >> mapM handleResponse responses) serv

majoritySuccessful :: [Message] -> Bool
majoritySuccessful resps = True -- (\resps -> length (filter wasSuccessful resps) > nCohorts / 2)


main :: IO ()
main = do
  args <- getArgs
  case args of
   (myId:"test":_) -> testMain . fromIntegral . read $ myId

   (myId:"leader":_) -> do
     let sid = fromIntegral . read $ myId
     conf <- readJSONConfig (kConfigFile sid) sid :: IO (ServerConfig JsonStorage HandleConnection String)
     serv <- fromPersist . initializeFollower $ conf
     client <- newTestSIClient
     void $ debugMain (leaderMain client) (promoteToLeader serv)

   (myId:_) -> do
     let sid = fromIntegral . read $ myId
     conf <- readJSONConfig (kConfigFile sid) sid :: IO (ServerConfig JsonStorage NilConnection String)
     serv <- fromPersist . initializeFollower $ conf
     void $ debugMain followerMain serv

   _ -> error "Invalid arguments."


-- Testing and testing utils

testMain :: ServerId -> IO ()
testMain myId = do
  ioConfig <- writeTestConfig simpleConfig >> readTestConfig

  appendLdr <- testLocalSystemWith (Log [LogEntry 1 "stardate one"]) myId
  appendLogs <- followerLogs appendLdr

  -- hbeatLdr <- testLocalSystemWith (Log []) myId
  -- hbeatLogs <- followerLogs hbeatLdr

  runTestTT . TestList $ [
        TestLabel "testConfig"
        . TestCase $ assertEqual "for write >> read config," (Just simpleConfig)
        ioConfig
        ,
        TestLabel "testSimpleAppendResponses"
        . TestCase $ assertEqual ("for matchIndices @ testLocalSystem " ++ show myId ++ ",") (Just [1,1,1])
        (liftM (map snd . Map.toList) $ view matchIndex appendLdr)
        ,
        TestLabel "testSimpleAppendStorage"
        . TestCase $ assertAllEqual ("for follower logs @ testLocalSystem " ++ show myId ++ ",") (view log appendLdr)
        appendLogs
        ]

  print "All done!"
    where third (_, _, x) = x
          followerLogs ldr = do
              followerStorages <- mapM (selfConnectionStorage . snd) . Map.toList . view (config.cohorts) $ ldr
              mapM (fmap third . readFromStable) followerStorages

assertAllEqual :: (Eq a, Show a) => String -> a -> [a] -> Assertion
assertAllEqual msg expected = mapM_ (assertEqual msg expected)

{-# NOINLINE debug #-}
debug :: String -> a -> a
debug err dat = unsafePerformIO (putStrLn err) `seq` dat
-- debug _ = id

{-# NOINLINE debug' #-}
debug' :: Show a => a -> a
debug' dat = unsafePerformIO (print dat) `seq` dat

data SelfConnection s c a = SelfConnection (IORef (Server s c a)) (IORef MessageId)

selfConnectionStorage :: SelfConnection s c a -> IO (s a)
selfConnectionStorage (SelfConnection servRef _) = view (config.storage) <$> readIORef servRef

instance (ToJSON a, FromJSON a, Persist s) => Connection (SelfConnection s c a) where
  request req (SelfConnection servRef _) = do
    serv <- readIORef servRef
    let (resp, serv') = runState (handleRequest req) serv
    persist serv' >> writeIORef servRef serv' >> return (Just resp)
  respond resp (SelfConnection servRef _) = do
    serv <- readIORef servRef
    let serv' = execState (handleResponse resp) serv
    persist serv' >> writeIORef servRef serv' >> return ()

  listen (SelfConnection servRef midRef) = do
    serv <- readIORef servRef
    requestInfo midRef serv >>= return . Just . appendEntriesFromLeader serv (logWithIndices serv)

  fromConfig conf@(CohortConfig sid host port) = do
    mid <- newIORef 0
    serv <- newIORef . initializeFollower $ ServerConfig Follower conf Map.empty (fromName storageName)
    return $ SelfConnection serv mid
    where storageName = kLogDir ++ host ++ "_" ++ show port ++ ".local.json"

instance Show a => Show (SelfConnection s c a) where
         show (SelfConnection servRef _) = show . unsafePerformIO . readIORef $ servRef

simpleConfig :: ClusterConfig
simpleConfig = ClusterConfig {
    _clusterLeader = 1,
    _clusterServers = [
      CohortConfig 1 "localhost" 3001,
      CohortConfig 2 "localhost" 3002,
      CohortConfig 3 "localhost" 3003
      ]
    }

testLocalSystemWith :: Log String -> ServerId -> IO (Server JsonStorage (SelfConnection JsonStorage NilConnection String) String)
testLocalSystemWith lg myId = do
  conf <- readJSONConfig (kConfigFile myId) myId
  let serv = initializeFollower conf
  case view role conf of
   Leader -> leaderMain AbortClient . injectPersistentState (1, Just myId, lg) . promoteToLeader $ serv
   Follower -> followerMain serv


writeTestConfig :: ClusterConfig -> IO ()
writeTestConfig = ByteString.writeFile (kConfigDir ++ "config.auto.json") . encode

readTestConfig :: IO (Maybe ClusterConfig)
readTestConfig = decode <$> ByteString.readFile (kConfigDir ++ "config.auto.json")

testManual :: IO ()
testManual = do
  nextMessageId <- newIORef 1
  print "Test: sending messages."
  let me = promoteToLeader $ initializeFollower fakeConf
  prepareBroadcast nextMessageId append me >>= broadcast >>= print
  readIORef nextMessageId >>= print . (++) "Next message id: " . show
  print "Done."

  print "Test: receiving messages."
  let me' = demoteToFollower me :: Server JsonStorage FakeConnection String
  req <- fmap fromJust . listen $ FakeConnection
  let (resp, me'') = runState (processAppendEntries req) me'
  print resp
  print (view log me'')
  print "Done."
    where fakeConf = ServerConfig Booting myConf followers (JsonStorage $ kLogDir ++ "test.json")
          myConf = CohortConfig 1 "localhost" 1001
          followers = Map.fromList [
            (1, FakeConnection),
            (2, FakePartition),
            (3, FakeConnection),
            (4, FakePartition),
            (5, FakeConnection)
            ]
          append :: BaseMessage
          append = appendEntries 11 0 1 2 (zip [1..] myLog) 3
          myLog = [] :: [LogEntry String]
