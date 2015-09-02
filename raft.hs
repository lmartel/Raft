{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ExistentialQuantification #-}
module Raft where
import qualified Prelude (log)
import Prelude hiding (log)
import System.Environment (getArgs)
import Control.Lens
import Control.Monad
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

import Test.HUnit

import RaftTypes
import MessageTypes
import ConnectionTypes
import JsonStorage
import Config (CohortConfig, ClusterConfig)
import qualified Config

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

requestInfo :: IORef MessageId -> Server s c a -> IO MessageInfo
requestInfo midRef serv = readIORef midRef >>= writeNextMid
  where writeNextMid mid = atomicWriteIORef midRef (mid + 1) >> return (MessageInfo (view (config.serverId) serv) mid)

responseInfo :: Message -> Server s c a -> MessageInfo
responseInfo req serv = MessageInfo (view (config.serverId) serv) (view (info.msgId) req)

prepareBroadcast :: IORef MessageId -> BaseMessage -> Server s c a -> IO [PendingMessage c]
prepareBroadcast midRef msg serv
  | view (config.role) serv /= Leader = return []
  | otherwise = readIORef midRef >>= writeNextMid . requests
  where writeNextMid [] = return []
        writeNextMid reqs = atomicWriteIORef midRef (nextMid reqs) >> return reqs
        nextMid :: [PendingMessage c] -> MessageId
        nextMid reqs = (1 + ) . last $ map (view (info.msgId) . snd) reqs
        requests mid1 = zipWith prepareMessage (serverCohorts serv) [mid1..]
        prepareMessage c mid = (c, msg $ MessageInfo (view (config.serverId) serv) mid)

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
   -- TODO requestvote


-- TODO figure out maybeT
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
                                           . over matchIndex (fmap $ Map.insert responder matchedId)
                                           . over nextIndex (fmap . Map.insert responder $ matchedId + 1)
                                           $ me
  where respId = view (info.msgId) msg
        responder = view (info.msgFrom) msg

        lastSentIndex :: [(LogIndex, LogEntry NilEntry)] -> Maybe LogIndex
        lastSentIndex [] = Nothing
        lastSentIndex es = Just . fst . last $ es

handleRequestVoteResponse :: Message -> Bool -> Raft s c a ()
handleRequestVoteResponse = undefined
-- retryIfNeeded ::


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
          | otherwise = error "processAppendEntries :: Error, there's a hole in the log!" -- TODO address this error

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
kConfigDir = "db/"
kConfigFile :: String
kConfigFile = kConfigDir ++ "config.json"

kLogDir :: String
kLogDir = kConfigDir
kLogFile :: String
kLogFile = kLogDir ++ "log.json"

configureCohorts :: Connection c => ClusterConfig -> IO (ServerMap c)
configureCohorts conf = liftM Map.fromList $ pure conf
                        >>= connectAll . map (\conf@(Config.CohortConfig sid _ _) -> (sid, fromConfig conf)) . Config.servers
                   where connectAll :: [(a, IO b)] -> IO [(a, b)]
                         connectAll connections = return . zip (map fst connections) =<< mapM snd connections

configureSelf :: Connection c => ServerId -> ClusterConfig -> s a -> IO (ServerConfig s c a)
configureSelf myId cluster stor = liftM (\clust' -> ServerConfig myId myRole clust' stor) (configureCohorts cluster)
  where myRole = if myId == Config.leader cluster
                 then Leader
                 else Follower

-- TODO: generalize log entry type (use reflection?)
readJSONConfig :: (Persist s, Connection c) => String -> ServerId -> IO (ServerConfig s c String)
readJSONConfig f myId = ByteString.readFile f >>= \confStr -> case decode confStr of
                              Nothing -> return . error $ "cannot read or parse config file: " ++ f
                              (Just conf) -> configureSelf myId conf (fromName kLogFile)

serverMain :: ServerId -> IO ()
serverMain myId = undefined
-- serverMain myId = do
--   conf <- readJSONConfig kConfigFile myId :: IO (ServerConfig JsonStorage FakeConnection String)
--   case view role conf of
--    Leader -> leaderMain conf
--    Follower -> followerMain conf

singleThreadedFollowerMain :: Connection c => ServerConfig s c a -> IO ()
singleThreadedFollowerMain = undefined

followerMain :: Connection c => ServerConfig s c a -> IO (Server s c a)
followerMain conf = do
  let nCohorts = Map.size . view cohorts $ conf
  -- create socket
  sock <- socket AF_INET Stream 0
  -- make socket immediately reusable - eases debugging.
  setSocketOption sock ReuseAddr 1
  -- listen on TCP port
  bindSocket sock (SockAddrInet (fromIntegral port) iNADDR_ANY)
  -- allow a maximum of (#COHORTS) outstanding connections
  Sock.listen sock nCohorts
  replicate nCohorts (accept sock)


leaderMain :: (Connection c, ToJSON a, FromJSON a, Show a) => PersistentState a -> ServerConfig s c a -> IO (Server s c a)
leaderMain stableState@(_, _, startingLog) conf = do
  nextMessageId <- newIORef 0

  let serv = injectPersistentState stableState . promoteToLeader . initializeFollower $ conf
  let nCohorts = Map.size . view (config.cohorts) $ serv

  putStrLn "===== LEADER STARTING ====="
  print serv

  let req = appendEntriesFromLeader serv (withIndices startingLog)
  requests <- prepareBroadcast nextMessageId req serv
  responses <- broadcastUntil majoritySuccessful requests
  let serv' = execState (expectResponsesTo requests >> mapM handleResponse responses) serv

  putStrLn "===== LEADER FINISHED ====="
  print serv'
  putStrLn "leaderMain done."
  return serv'
  where majoritySuccessful :: [Message] -> Bool
        majoritySuccessful resps = True -- (\resps -> length (filter wasSuccessful resps) > nCohorts / 2)

singleThreadedLeaderMain :: IO ()
singleThreadedLeaderMain = undefined

main :: IO ()
main = do
  args <- getArgs
  (case args of
   (myId:"test":_) -> testMain . fromIntegral . read $ myId
   (myId:_) -> serverMain . fromIntegral . read $ myId
   _ -> error "Invalid arguments." -- TODO fancier arg parsing / flags
   )

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
debug err dat = unsafePerformIO (print err) `seq` dat
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

  -- TODO myId `mod` 2 to determine which RPC to send
  listenMaybe (SelfConnection servRef midRef) = do
    serv <- readIORef servRef
    requestInfo midRef serv >>= return . Just . appendEntriesFromLeader serv (logWithIndices serv)

  fromConfig (Config.CohortConfig sid host port) = do
    mid <- newIORef 0
    serv <- newIORef . initializeFollower $ ServerConfig sid Follower Map.empty (fromName storageName)
    return $ SelfConnection serv mid
    where storageName = kLogDir ++ host ++ "_" ++ show port ++ ".local.json"

-- TODO get rid of this
instance Show a => Show (SelfConnection s c a) where
         show (SelfConnection servRef _) = show . unsafePerformIO . readIORef $ servRef

simpleConfig :: ClusterConfig
simpleConfig = Config.ClusterConfig {
    Config.leader = 1,
    Config.servers = [
      Config.CohortConfig 1 "localhost" 3001,
      Config.CohortConfig 2 "localhost" 3002,
      Config.CohortConfig 3 "localhost" 3003
      ]
    }

testLocalSystemWith :: Log String -> ServerId -> IO (Server JsonStorage (SelfConnection JsonStorage NilConnection String) String)
testLocalSystemWith lg myId = do
  conf <- readJSONConfig kConfigFile myId
  case view role conf of
   Leader -> leaderMain (1, Just myId, lg) conf
   Follower -> followerMain conf


writeTestConfig :: ClusterConfig -> IO ()
writeTestConfig = ByteString.writeFile (kConfigDir ++ "config.auto.json") . encode

readTestConfig :: IO (Maybe ClusterConfig)
readTestConfig = decode <$> ByteString.readFile kConfigFile

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
  req <- listen FakeConnection
  let (resp, me'') = runState (processAppendEntries req) me'
  print resp
  print (view log me'')
  print "Done."
    where fakeConf = ServerConfig 0 Booting followers (JsonStorage $ kLogDir ++ "test.json")
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
