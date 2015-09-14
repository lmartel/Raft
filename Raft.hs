{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}
module Raft where
import qualified Prelude (log)
import Prelude hiding (log)
import System.Environment (getArgs)
import Control.Concurrent hiding (putMVar)
import qualified Control.Concurrent as LazyConcurrency (putMVar)
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
import Debug

--- The strict-concurrency package is out-of-date, so we roll our own:

putMVar :: MVar a -> a -> IO ()
putMVar box !v = LazyConcurrency.putMVar box v

--- Initialization

initializeFollower :: ServerConfig cl s c a -> Server cl s c a
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

promoteToLeader :: Server cl s c a -> Server cl s c a
promoteToLeader base = set (config.role) Leader . set nextIndex (Just nis) . set matchIndex (Just mis) $ base
  where nis :: ServerMap LogIndex
        nis = Map.map (\_ -> viewLastLogIndex base) . view (config.cohorts) $ base
        mis :: ServerMap LogIndex
        mis = Map.map (\_ -> LogIndex 0) . view (config.cohorts) $ base

nominateToCandidate :: Server cl s c a -> Server cl s c a
nominateToCandidate = set (config.role) (Candidate Map.empty) . demoteToFollower

demoteToFollower :: Server cl s c a -> Server cl s c a
demoteToFollower = set (config.role) Follower . set nextIndex Nothing . set matchIndex Nothing

--- Sending/receiving messages

atomicGetNextMessageId :: IORef MessageId -> IO MessageId
atomicGetNextMessageId ref = atomicModifyIORef ref (\mid -> (mid + 1, mid))

atomicGetNextMessageIds :: Int -> IORef MessageId -> IO [MessageId]
atomicGetNextMessageIds n ref = atomicModifyIORef ref (\mid -> (mid + fromIntegral n, take n [mid..]))

requestInfo :: IORef MessageId -> Server cl s c a -> IO MessageInfo
requestInfo midRef serv = MessageInfo (view serverId serv) <$> atomicGetNextMessageId midRef

responseInfo :: Message -> Server cl s c a -> MessageInfo
responseInfo req serv = MessageInfo (view serverId serv) (view (info.msgId) req)

broadcast :: Connection c => IORef MessageId -> BaseMessage -> Server cl s c a -> IO (Server cl s c a)
broadcast midRef msg serv
  | view (config.role) serv /= Leader = return serv
  | otherwise = do
      msgs <- requests <$> atomicGetNextMessageIds (length . serverCohorts $ serv) midRef
      let serv' = execState (expectResponsesTo . map snd $ msgs) serv
      mapM_ sendPending msgs
      return serv'
  where requests = zipWith prepareMessage (serverCohorts serv)
        prepareMessage c mid = (c, msg $ MessageInfo (view serverId serv) mid)
        sendPending = uncurry . flip $ respond

expectResponsesTo :: [Message] -> Raft cl s c a ()
expectResponsesTo msgs = get >>= put . over outstanding multiInsert
  where multiInsert msgMap = foldl singleInsert msgMap msgs
        singleInsert :: Map MessageId Message -> Message -> Map MessageId Message
        singleInsert msgMap pendMsg = Map.insertWithKey dupeInsert (view (info.msgId) pendMsg) pendMsg msgMap
        dupeInsert k _ _ = error $ "expectResponsesTo :: second message sent with id " ++ show k

--- Sending messages from Leader to self-as-Follower

instance (ToJSON a, FromJSON a, Show a) => Connection (SelfConnection (Server cl s c a)) where
  fromConfig = undefined

  respond msg (SelfConnection servBox respQ qNotEmpty) = do
    serv <- takeMVar servBox
    let (mResp, serv') = runState (handleRequest msg) serv
    putMVar servBox serv'
    maybe (return ()) (\resp -> snocQueue respQ resp >> tryPutMVar qNotEmpty () >> return ()) mResp

  listen (SelfConnection _ respQ qNotEmpty) = do
    (nxt:rest) <- takeMVar qNotEmpty >> takeMVar respQ
    case rest of
     [] -> return ()
     _ -> putMVar qNotEmpty ()

    putMVar respQ rest >> return (Just nxt)


--- Raft algorithm core

handleRequest :: (FromJSON a) => Message -> Raft cl s c a (Maybe Message)
handleRequest msg = case view msgType msg of
  AppendEntries -> debug' "AppendEntries. " (Just <$> processAppendEntries msg)
  RequestVote -> debug' "RequestVote." (Just <$> processRequestVote msg)

handleResponse :: (ClientConnection cl a, FromJSON a, Show a) => Message -> Raft cl s c a ()
handleResponse msg = do
  let maybeMalformed = [
        liftM checkOutOfDate (term msg),
        handleResponse'
        ]
  case sequence maybeMalformed of
   Nothing -> return ()
   (Just states) -> sequence_ states
  where checkOutOfDate :: Term -> Raft cl s c a ()
        checkOutOfDate followerTerm = do
             me <- get
             if view currentTerm me < followerTerm
               then put . set currentTerm followerTerm . demoteToFollower $ me
               else put me

        handleResponse' :: (ClientConnection cl a, FromJSON a, Show a) => Maybe (Raft cl s c a ())
        handleResponse' = case view msgType msg of
                           AppendEntriesResponse -> debug' "AppendEntriesResponse." $
                                                    liftM (handleAppendEntriesResponse msg) (success msg)
                           RequestVoteResponse -> debug' "RequestVoteResponse." $
                                                  liftM (handleRequestVoteResponse msg) $ voteGranted msg

-- On failure: decrement nextIndex, retry later
-- On success: update matchIndex and nextIndex, then resolve (delete) the pending message.
handleAppendEntriesResponse :: (ClientConnection cl a, Show a) => Message -> Bool -> Raft cl s c a ()
handleAppendEntriesResponse msg False = do
  me <- get
  put $ over nextIndex (fmap $ Map.insertWith (\_ old -> max (old - 1) 0) (view (info.msgFrom) msg) (1 + view commitIndex me)) me


handleAppendEntriesResponse msg True = do
  me <- get
  case Map.lookup respId $ view outstanding me of
   Nothing -> debug ("Got response to message (ID: "
                     ++ show respId
                     ++ ") never sent or already resolved. Ignoring.")
              (put me)
   (Just req) -> case lastSentIndex =<< entries req of
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

-- This reports to the client that newly-committed entries are committed.
-- This is safe to do immediately, because "committed" means the entry has been written to
--     disk by a majority of followers;
-- The leader does not need to persist anything to make the commit "official."

-- TODO: refactor with Monad transformers to allow a "committed" client IO callback here.
checkCommitted :: (ClientConnection cl a, Show a) => Maybe [LogIndex] -> Server cl s c a -> Server cl s c a
checkCommitted Nothing serv0 = serv0
checkCommitted (Just xs) serv0 = let toCommit = filter (canCommit serv0) xs in
                                  over commitIndex (\old -> foldl max old toCommit) serv0
  where canCommit :: Server cl s c a -> LogIndex -> Bool
        canCommit serv n = n > view commitIndex serv
                           && majorityMatched n serv
                           && termAtIndex n serv == Just (view currentTerm serv)

        doCommits :: (ClientConnection cl a, Show a) => Server cl s c a -> [LogIndex] -> IO ()
        doCommits serv ns = mapM_ (commitTo serv) (debugPrint $ getEntriesForIndices serv ns)

        getEntriesForIndices :: Show a => Server cl s c a -> [LogIndex] -> [LogEntry a]
        getEntriesForIndices serv = mapMaybe (flip entry $ view log serv)

        commitTo :: (ClientConnection cl a, Show a) => Server cl s c a -> LogEntry a -> IO ()
        commitTo serv entr = view entryData entr `committed` view (config.client) serv



majorityMatched :: LogIndex -> Server cl s c a -> Bool
majorityMatched n serv = case view matchIndex serv of
  Nothing -> False
  (Just mp) -> 2 * Map.size (Map.filter (>= n) mp)
               > Map.size mp

handleRequestVoteResponse :: Message -> Bool -> Raft cl s c a ()
handleRequestVoteResponse resp vote = do
  me <- get
  case view (config.role) me of
   (Candidate votes) -> let votes' = Map.insert (view (msgInfo.msgFrom) resp) vote votes
                            me' = set (config.role) (Candidate votes') me
                        in put me'
                           --   if wasElected votes' me'
                             --   then return () -- TODO
                               -- else return ()
   _ -> return ()
  where wasElected :: ServerMap Bool -> Server cl s c a -> Bool
        wasElected votes me = 2 * (Map.size . Map.filter (== True) $ votes)
                              > ((+ 1) . Map.size $ view (config.cohorts) me)

processAppendEntries :: FromJSON a => Message -> Raft cl s c a Message
processAppendEntries msg = do
  me <- get
  case validateAppendEntries me msg of
   Nothing -> do put me
                 debug "Failed validation." $ return (response False me)
   (Just entrs) -> let me' = set currentTerm (fromJust $ term msg)
                             . over log (updateLogWith entrs)
                             . set commitIndex (updateCommitIndex me $ leaderCommit msg)
                             $ me
                   in do put me'
                         debug "Succeeded!" $ return (response True me')
  where updateCommitIndex :: Server cl s c a -> Maybe LogIndex -> LogIndex
        updateCommitIndex me Nothing = view commitIndex me
        updateCommitIndex me (Just theirs) = min theirs (viewLastLogIndex me)

        response :: Bool -> Server cl s c a -> Message
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
          | i < 1 + lastIndex lg  = appendNew es lg
          | i == 1 + lastIndex lg = appendNew es $ over logEntries (++ [e]) lg
          | otherwise             = debug ("processAppendEntries :: Error, there's a hole in the log! Last index: "
                                           ++ show (lastIndex lg) ++ "; Entry index: " ++ show i) lg

validateAppendEntries :: FromJSON a => Server cl s c a -> Message -> Maybe [IndexedEntry a]
validateAppendEntries serv msg =  case Just . all (== True) =<< sequence [
  debugUnlessM "Term too old. " $ (>= view currentTerm serv) <$> term msg,
  debugUnlessM "Mismatched term at prev index. " $ noPrevLogEntries `maybeOr` prevLogEntryMatches
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

processRequestVote :: FromJSON a => Message -> Raft cl s c a Message
processRequestVote msg = do
  me <- get
  case validateRequestVote me msg of
   Nothing -> debug "Failed validation." $ return (response False me)
   (Just cid) -> let me' = set currentTerm (fromJust $ term msg)
                           . set votedFor (Just cid)
                           $ me
                 in do put me'
                       debug "Succeeded!" $ return (response True me')
  where response :: Bool -> Server cl s c a -> Message
        response b me = requestVoteResponse (view currentTerm me) b $ responseInfo msg me


validateRequestVote :: FromJSON a => Server cl s c a -> Message -> Maybe ServerId
validateRequestVote serv msg = case Just . all (== True) =<< sequence [
  debugUnlessM "Already voted for another candidate." $ canVote (view votedFor serv) <$> candidateId msg,
  debugUnlessM "Term too old." $ (>= view currentTerm serv) <$> term msg,
  debugUnlessM "Candidate log out of date." $ candidateLogFresh <$> lastLogIndex msg <*> lastLogTerm msg
  ] of
                                (Just True) -> candidateId msg
                                _ -> Nothing
  where canVote Nothing _ = False
        canVote (Just vid) cid = vid == cid

        localTerm = view currentTerm serv

        candidateLogFresh :: LogIndex -> Term -> Bool
        candidateLogFresh cLogIndex cLogTerm
          | cLogTerm > localTerm  = True
          | cLogTerm == localTerm = cLogIndex >= viewLastLogIndex serv
          | otherwise             = False
--- Startup and main

kConfigDir :: String
kConfigDir = "conf/"
kConfigFile :: ServerId -> String
kConfigFile (ServerId sid) = kConfigDir ++ "config.json"

kLogDir :: String
kLogDir = "db/"
kLogFile :: ServerId -> String
kLogFile (ServerId sid) = kLogDir ++ "log." ++ show sid ++ ".json"

configureCohorts :: Connection c => ClusterConfig -> IO (ServerMap c)
configureCohorts conf = liftM Map.fromList $ pure conf
                        >>= connectAll . map (\conf@(CohortConfig sid _ _) -> (sid, fromConfig conf)) . view clusterServers
                   where connectAll :: [(a, IO b)] -> IO [(a, b)]
                         connectAll connections = return . zip (map fst connections) =<< mapM snd connections

configureSelf :: (ClientConnection cl a, Connection c) => CohortConfig -> ClusterConfig -> s a -> IO (ServerConfig cl s c a)
configureSelf myConf cluster stor = do
  self <- Just <$> newOwnFollower
  clust' <- configureCohorts cluster
  ServerConfig myRole myConf self clust' stor <$> (fromClientConfig $ view clientConfig cluster)
  where myRole = if view cohortId myConf == view clusterLeader cluster
                 then Leader
                 else Follower

readJSONConfig :: (ClientConnection cl a, Persist s, Connection c) => String -> ServerId -> IO (ServerConfig cl s c a)
readJSONConfig f myId = do
  confStr <- ByteString.readFile f
  let maybeConf = decode confStr >>= (\clust -> liftM2 (,) (Just clust) (myConf clust))
  case maybeConf of
   Nothing -> return . error $ "cannot read or parse config file: " ++ f
   (Just (clust, me)) -> configureSelf me (filterMe myId clust) (fromName . kLogFile $ view cohortId me)
  where myConf :: ClusterConfig -> Maybe CohortConfig
        myConf = find (\someConf -> view cohortId someConf == myId) . view clusterServers

        filterMe :: ServerId -> ClusterConfig -> ClusterConfig
        filterMe sid = over clusterServers (filter ((/= sid) . view cohortId))

debugMain :: (Show c, Show a) => (Server cl s c a -> IO (Server cl s c a)) -> Server cl s c a -> IO (Server cl s c a)
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


type ServerWorker cl s c a = (MVar (Server cl s c a)) -> IO ()
type ServerListener cl s c a = Handle -> ServerWorker cl s c a

-- takeMVar' :: MVar a -> IO a
-- takeMVar' mv = fromJust <$> tryTakeMVar mv

withListeners :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                 Server cl s c a -> ServerListener cl s c a -> [ServerWorker cl s c a] -> IO (Server cl s c a)
withListeners serv0 acceptFn otherFns = do
  let initialRole = view (config.role) serv0
  serverState <- newEmptyMVar
  workerThreads <- newMVar []
  allDone <- newEmptyMVar

  -- create socket
  sock <- socket AF_INET Stream 0
  -- make socket immediately reusable - eases debugging.
  setSocketOption sock ReuseAddr 1
  -- listen on TCP port
  bindSocket sock (SockAddrInet (fromIntegral . view (config.ownCohort.cohortPort) $ serv0) iNADDR_ANY)
  -- allow a maximum of (#COHORTS) outstanding connections
  Sock.listen sock 5

  installHandler keyboardSignal (Catch $ cleanupAndExit serverState workerThreads allDone) Nothing
  snocQueue workerThreads =<< forkIO (spawnerThread sock serverState workerThreads)

  mapM_ (\f -> forkIO (f serverState) >>= snocQueue workerThreads) otherFns
  putMVar serverState serv0
  takeMVar allDone
  cleanupAndExit serverState workerThreads allDone
  close sock -- TODO figure out how to close properly and uninstall keyboard handler, or just recycle them
  serv' <- takeMVar serverState
  let newRole = view (config.role) serv'
    in if initialRole /= newRole
       then debug ("Restaring with new role: " ++ show newRole) mainForRole serv'
       else return serv'
  where spawnerThread sock serverState workerThreads = do
          (sock', _) <- accept sock
          hdl <- debug "Listener accepted connection." $ socketToHandle sock' ReadWriteMode
          hSetBuffering hdl NoBuffering

          snocQueue workerThreads =<< forkIO (acceptFn hdl serverState)
          spawnerThread sock serverState workerThreads

cleanupAndExit :: (Show a) => MVar (Server cl s c a) -> MVar [ThreadId] -> MVar () -> IO ()
cleanupAndExit serverState workersQueue done = do
  serverLock <- takeMVar serverState
  workers <- takeMVar workersQueue
  unless (null workers) $
    debug "Workers killed." . mapM_ killThread . debug "Cleaning up..." $ workers

  putMVar workersQueue []
  putMVar serverState serverLock
  putMVar done ()

snocQueue :: MVar [a] -> a -> IO ()
snocQueue q v = takeMVar q >>= putMVar q . (++ [v])


-- Type signature commented out because it needs an explicit FORALL to compile properly and, well, fuck that
-- listenerFromHandle :: Connection conn => (conn -> MVar (Server cl s c a) -> IO ()) -> Handle -> MVar (Server cl s c a) -> IO ()
listenerFromHandle listener hdl servBox = forever $ listener (SimpleHandleConnection hdl) servBox

-- Listen for a message, check for relevance, process it.
listenForSomethingOnce :: (ClientConnection cl a, Persist s, Connection conn, FromJSON a, ToJSON a, Show a) =>
                          (Message -> Bool) -> (Message -> Raft cl s c a (Maybe Message)) ->
                          conn -> MVar (Server cl s c a) -> IO ()
listenForSomethingOnce shouldHandle handler conn servBox = do
  maybeReq <- listen conn
  maybe retryLater (maybeProcess . debug' "Received request... ") maybeReq
    where doMaybe :: (a -> IO ()) -> Maybe a -> IO ()
          doMaybe = maybe $ return ()

          retryLater :: IO ()
          retryLater = threadDelay (kReconnect * 1000)

          maybeProcess :: Message -> IO ()
          maybeProcess req
            | shouldHandle req = do
                serv <- takeMVar servBox
                let (resp, serv') = runState (handler req) serv
                persist serv'

                putMVar servBox serv'
                doMaybe (`respond` conn) resp
            | otherwise = debug ("Suppressing unexpected message of type " ++ show (view msgType req)) return ()

-- Used by Follower coordinator thread to listen for requests (only).
listenForRequestOnce :: (ClientConnection cl a, Persist s, Connection conn, FromJSON a, ToJSON a, Show a) =>
                        conn -> MVar (Server cl s c a) -> IO ()
listenForRequestOnce = listenForSomethingOnce (isRequest . view msgType) handleRequest

-- TODO use `forever` at toplevel instead of recursion here
-- Used on the Leader only to listen for responses (only).
listenForResponseOnce :: (ClientConnection cl a, Persist s, Connection conn, FromJSON a, ToJSON a, Show a) =>
                         conn -> MVar (Server cl s c a) -> IO ()
-- listenForResponseOnce = debug "Listening for response..." $ listenForSomethingOnce (isResponse . view msgType)
listenForResponseOnce conn servBox = do
  listenForSomethingOnce (isResponse . view msgType) (\resp -> handleResponse resp >> return Nothing) conn servBox
  listenForResponseOnce conn servBox

--- Main functions for each role

mainForRole :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
               Server cl s c a -> IO (Server cl s c a)
mainForRole serv0 = case view (config.role) serv0 of
  Follower -> followerMain serv0
  Leader -> leaderMain serv0
  (Candidate _) -> candidateMain serv0


followerMain :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                Server cl s c a -> IO (Server cl s c a)
followerMain serv0 = withListeners serv0 (listenerFromHandle listenForRequestOnce) []

candidateMain :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                 Server cl s c a -> IO (Server cl s c a) -- TODO: after successful request, downgrade Role
candidateMain serv0 = withListeners serv0
                      (listenerFromHandle listenForRequestOnce)
                      (voteForMe : selfListener serv0 ++ cohortListeners serv0)
  where voteForMe = undefined


leaderMain :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
              Server cl s c a -> IO (Server cl s c a)
leaderMain serv0 = withListeners serv0
                   (listenerFromHandle listenForRequestOnce)
                   (broadcastThread : commitReporter : selfListener serv0 ++ cohortListeners serv0)

commitReporter :: (ClientConnection cl a, Show a) => ServerWorker cl s c a
commitReporter servBox = do
  serv <- takeMVar servBox
  let latestCommitted = view entryData <$> view commitIndex serv `entry` view log serv
  maybe (return ()) (`committed` view (config.client) serv) latestCommitted
  putMVar servBox serv
  threadDelay kCommitReport
  commitReporter servBox

selfListener :: (ClientConnection cl a, Persist s, FromJSON a, ToJSON a, Show a) =>
                Server cl s c a -> [ServerWorker cl s c a]
selfListener serv0 = case view (config.ownFollower) serv0 of
                Nothing -> []
                (Just self) -> [\servBox -> listenForResponseOnce (selfConnection servBox self) servBox]


cohortListeners :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                   Server cl s c a -> [ServerWorker cl s c a]
cohortListeners serv = map listenForResponseOnce $ cohortConnections serv
  where cohortConnections :: Server cl s c a -> [c]
        cohortConnections = map snd . Map.toList . view (config.cohorts)

broadcastThread :: (ClientConnection cl a, Connection c, ToJSON a, FromJSON a, Show a) => MVar (Server cl s c a) -> IO ()
broadcastThread servBox = do
  nextMessageId <- newIORef 0 -- TODO: need to persist nextMessageId?
  serv0 <- takeMVar servBox
  let catchUp = appendEntriesFromLeader serv0 . logWithIndices $ serv0
  serv' <- broadcast nextMessageId catchUp serv0
  putMVar servBox serv'
  leaderLoop nextMessageId servBox

leaderLoop :: (ClientConnection cl a, Connection c, ToJSON a, FromJSON a, Show a) => IORef MessageId -> MVar (Server cl s c a) -> IO ()
leaderLoop nextMid servBox = do
  serv0 <- takeMVar servBox
  nextLogEntry <- getLogEntry $ view (config.client) serv0

  let serv = over log (logMap (++ [LogEntry (view currentTerm serv) nextLogEntry])) serv0
  let update = appendEntriesFromLeader serv [last . logWithIndices $ serv]
  let self = fromJust $ view (config.ownFollower) serv
  selfUpdate <- update <$> requestInfo nextMid serv

  serv' <- execState (expectResponsesTo [selfUpdate]) <$> broadcast nextMid update serv
  putMVar servBox serv'
  respond selfUpdate $ selfConnection servBox self

  leaderLoop nextMid servBox


main :: IO ()
main = do
  args <- getArgs
  case args of
   (myId:"test":_) -> testMain . fromIntegral . read $ myId

   (myId:"log":_) -> do
     let sid = fromIntegral . read $ myId
     conf <- readJSONConfig (kConfigFile sid) sid :: IO (ServerConfig SimpleIncrementingClient JsonStorage NilConnection String)
     lg <- view log <$> (fromPersist . initializeFollower $ conf)
     logMain sid lg

   (myId:"leader":_) -> do
     let sid = fromIntegral . read $ myId
     conf <- readJSONConfig (kConfigFile sid) sid :: IO (ServerConfig SimpleIncrementingClient JsonStorage HandleConnection String)
     serv <- fromPersist . initializeFollower $ conf
     void $ debugMain leaderMain (promoteToLeader serv)

   (myId:_) -> do
     let sid = fromIntegral . read $ myId
     conf <- readJSONConfig (kConfigFile sid) sid :: IO (ServerConfig SimpleIncrementingClient JsonStorage NilConnection String)
     serv <- fromPersist . initializeFollower $ conf
     void $ debugMain followerMain serv

   _ -> error "Invalid arguments."


-- Testing and testing utils

logMain :: Show a => ServerId -> Log a -> IO ()
logMain sid log = do
  putStrLn $ "Inspecting log for " ++ show sid
  putStrLn $ "Total entries: " ++ (show . length . view logEntries $ log)
  mapM_ (putStrLn . ppLogEntry) (withIndices log)
      where ppLogEntry :: Show a => (LogIndex, LogEntry a) -> String
            ppLogEntry (i, LogEntry trm dat) = show i ++ " | " ++ show trm ++ " | " ++ show dat


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

data InMemoryConnection cl s c a = InMemoryConnection (IORef (Server cl s c a)) (IORef MessageId)

selfConnectionStorage :: InMemoryConnection cl s c a -> IO (s a)
selfConnectionStorage (InMemoryConnection servRef _) = view (config.storage) <$> readIORef servRef

instance (ToJSON a, FromJSON a, Show a, Persist s) => Connection (InMemoryConnection AbortClient s c a) where
  request req (InMemoryConnection servRef _) = do
    serv <- readIORef servRef
    let (resp, serv') = runState (handleRequest req) serv
    persist serv' >> writeIORef servRef serv' >> return resp
  respond resp (InMemoryConnection servRef _) = do
    serv <- readIORef servRef
    let serv' = execState (handleResponse resp) serv
    persist serv' >> writeIORef servRef serv' >> return ()

  listen (InMemoryConnection servRef midRef) = do
    serv <- readIORef servRef
    requestInfo midRef serv >>= return . Just . appendEntriesFromLeader serv (logWithIndices serv)

  fromConfig conf@(CohortConfig sid host port) = do
    mid <- newIORef 0
    fol <- newOwnFollower
    serv <- newIORef . initializeFollower $ ServerConfig Follower conf Nothing Map.empty (fromName storageName) AbortClient
    return $ InMemoryConnection serv mid
    where storageName = kLogDir ++ host ++ "_" ++ show port ++ ".local.json"

instance Show a => Show (InMemoryConnection cl s c a) where
         show (InMemoryConnection servRef _) = show . unsafePerformIO . readIORef $ servRef

simpleConfig :: ClusterConfig
simpleConfig = ClusterConfig {
    _clusterLeader = 1,
    _clientConfig = ClientConfig "localhost" 3000,
    _clusterServers = [
      CohortConfig 1 "localhost" 3001,
      CohortConfig 2 "localhost" 3002,
      CohortConfig 3 "localhost" 3003
      ]
    }

testLocalSystemWith :: Log String -> ServerId -> IO (Server AbortClient JsonStorage (InMemoryConnection AbortClient JsonStorage NilConnection String) String)
testLocalSystemWith lg myId = do
  conf <- readJSONConfig (kConfigFile myId) myId
  let serv = initializeFollower conf
  case view role conf of
   Leader -> leaderMain . injectPersistentState (1, Just myId, lg) . promoteToLeader $ serv


writeTestConfig :: ClusterConfig -> IO ()
writeTestConfig = ByteString.writeFile (kConfigDir ++ "config.auto.json") . encode

readTestConfig :: IO (Maybe ClusterConfig)
readTestConfig = decode <$> ByteString.readFile (kConfigDir ++ "config.auto.json")
