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
import Data.Time
import Control.Monad.State
import System.IO.Unsafe
import Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as ByteString
import System.Posix.Signals
import System.IO
import System.Random
import System.Exit
import Network.Socket hiding (listen)
import qualified Network.Socket as Sock (listen)

import Test.HUnit

import RaftTypes
import MessageTypes
import ConnectionTypes
import JsonStorage
import Config
import Debug
import ExitCodes
import Demo

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


demoteToFollower :: Server cl s c a -> Server cl s c a
demoteToFollower = set (config.role) Follower
                   . set (config.ownFollower) Nothing
                   . set nextIndex Nothing
                   . set matchIndex Nothing
                   . set outstanding Map.empty

promoteToLeader :: Server cl s c a -> IO (Server cl s c a)
promoteToLeader old = newOwnFollower >>= \fol -> return $
                                                 set (config.role) Leader
                                                 . set (config.ownFollower) (Just fol)
                                                 . set nextIndex (Just nis)
                                                 . set matchIndex (Just mis)
                                                 $ demoted
  where demoted = if view (config.role) old == Leader
                  then error "promoteToLeader :: server is already leader"
                  else demoteToFollower old

        nis :: ServerMap LogIndex
        nis = Map.map (\_ -> viewLastLogIndex demoted) . view (config.cohorts) $ demoted
        mis :: ServerMap LogIndex
        mis = Map.map (\_ -> LogIndex 0) . view (config.cohorts) $ demoted

-- Candidates can be re-nominated; the SelfConnection should be recycled in this case,
-- because the renomination does not restart the process.
nominateToCandidate :: Server cl s c a -> IO (Server cl s c a)
nominateToCandidate old = getOwnFollower >>= \fol -> return $
                                                 over currentTerm (+ 1)
                                                 . set votedFor Nothing
                                                 . set (config.ownFollower) fol
                                                 . set (config.role) (Candidate Map.empty)
                                                 . demoteToFollower
                                                 $ old
  where getOwnFollower = case view (config.role) old of
          (Candidate _) -> return $ view (config.ownFollower) old
          _ -> Just <$> newOwnFollower


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
  | view (config.role) serv == Follower
              = debug "Follower attempting to broadcast; suppressing." return serv
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

instance (Persist s, ToJSON a, FromJSON a, Show a) => Connection (SelfConnection (Server cl s c a)) where
  fromConfig = undefined

  respond msg (SelfConnection servBox respQ qNotEmpty) = do
    serv <- takeMVar servBox
    let (mResp, serv') = runState (handleRequest msg) serv
    persist serv'
    putMVar servBox serv'
    maybe (return ()) (\resp -> resp `seq` snocQueue respQ resp >> tryPutMVar qNotEmpty () >> return ()) mResp

  listen (SelfConnection _ respQ qNotEmpty) = do
    (nxt:rest) <- takeMVar qNotEmpty >> takeMVar respQ
    case rest of
     [] -> return ()
     _ -> putMVar qNotEmpty ()

    putMVar respQ rest >> return (Just nxt)

--- Raft algorithm core


checkOutOfDate :: Maybe Term -> Raft cl s c a Bool
checkOutOfDate Nothing = return False
checkOutOfDate (Just theirTerm) = do
  me <- get
  let outOfDate = view currentTerm me < theirTerm
  when outOfDate
    (put . set votedFor Nothing . set currentTerm theirTerm . demoteToFollower $ me)
  return outOfDate

-- Check and handle out-of-date, but process either way.
handleRequest :: (FromJSON a) => Message -> Raft cl s c a (Maybe Message)
handleRequest msg = fmap Just $ do
                    checkOutOfDate (term msg)
                    case debug' "Received request... " view msgType msg of
                     AppendEntries -> debug' "AppendEntries. " (processAppendEntries msg)
                     RequestVote -> debug' "RequestVote. " (processRequestVote msg)



-- Check and handle out-of-date, then process only if up to date.

-- TODO: debug-logging in the response handler is too noisy.
-- The debug calls are just disabled for now; eventually, I want them piped to a separate file
handleResponse :: (ClientConnection cl a, FromJSON a, Show a) => Message -> Raft cl s c a ()
handleResponse msg = checkOutOfDate (term msg) >>= flip unless handleResponse'
  where handleResponse' :: (ClientConnection cl a, FromJSON a, Show a) => Raft cl s c a ()
        handleResponse' = case view msgType msg of
                           AppendEntriesResponse -> -- debug' "Received AppendEntriesResponse. " $
                             doMaybe (handleAppendEntriesResponse msg) (success msg)
                           RequestVoteResponse -> -- debug' "Received RequestVoteResponse. " $
                             doMaybe (handleRequestVoteResponse msg) (voteGranted msg)

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
        doCommits serv ns = mapM_ (commitTo serv) (getEntriesForIndices serv ns)

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
   _ -> return ()

processAppendEntries :: FromJSON a => Message -> Raft cl s c a Message
processAppendEntries msg = do
  me <- get
  case validateAppendEntries me msg of
   Nothing -> return $ debugResponse False me
   (Just entrs) -> let me' = set currentTerm (assertJust "processAppendEntries :: validation error" $ term msg)
                             . over log (updateLogWith entrs)
                             . set commitIndex (updateCommitIndex me $ leaderCommit msg)
                             $ me
                   in do put me'
                         return $ debugResponse True me'
  where updateCommitIndex :: Server cl s c a -> Maybe LogIndex -> LogIndex
        updateCommitIndex me Nothing = view commitIndex me
        updateCommitIndex me (Just theirs) = min theirs (viewLastLogIndex me)

        debugResponse :: Bool -> Server cl s c a -> Message
        debugResponse False = debug "Failed validation." $ response False
        debugResponse True = debug "Success!" $ response True

        response :: Bool -> Server cl s c a -> Message
        response b me = appendEntriesResponse (view currentTerm me) b $ responseInfo msg me

        updateLogWith :: [IndexedEntry a] -> Log a -> Log a
        updateLogWith [] = debug' "(Heartbeat.) " id
        updateLogWith es = debug' ("(" ++ show (length es) ++ ".) ") appendNew es . deleteConflicted es

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
   Nothing -> return $ debugResponse False me
   (Just cid) -> let me' = set currentTerm (assertJust "processRequestVote :: validation error" $ term msg)
                           . set votedFor (Just cid)
                           $ me
                 in do put me'
                       return $ debugResponse True me'
  where response :: Bool -> Server cl s c a -> Message
        response b me = requestVoteResponse (view currentTerm me) b $ responseInfo msg me

        debugResponse :: Bool -> Server cl s c a -> Message
        debugResponse False = debug "Failed validation." $ response False
        debugResponse True = debug "Success!" $ response True

validateRequestVote :: FromJSON a => Server cl s c a -> Message -> Maybe ServerId
validateRequestVote serv msg = case Just . all (== True) =<< sequence [
  debugUnlessM "Already voted for another candidate." $ canVote (view votedFor serv) <$> candidateId msg,
  debugUnlessM "Term too old." $ (>= view currentTerm serv) <$> term msg,
  debugUnlessM "Candidate log out of date." $ candidateLogFresh <$> lastLogIndex msg <*> lastLogTerm msg
  ] of
                                (Just True) -> candidateId msg
                                _ -> Nothing
  where canVote Nothing _ = True
        canVote (Just vid) cid = vid == cid

        candidateLogFresh :: LogIndex -> Term -> Bool
        candidateLogFresh cLogIndex cLogTerm
          | cLogTerm > viewLastLogTerm serv  = True
          | cLogTerm == viewLastLogTerm serv = cLogIndex >= viewLastLogIndex serv
          | otherwise                        = False
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
  clust' <- configureCohorts cluster
  ServerConfig Follower myConf Nothing clust' stor <$> (fromClientConfig $ view clientConfig cluster)

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

debugServInfo :: (Show a) => Server cl s c a -> String
debugServInfo serv = show (view serverId serv) ++ " " ++ show (view (config.role) serv)

debugMainStarting :: (Show a) => Server cl s c a -> IO ()
debugMainStarting serv0 = do
  debugIO $ "===== " ++ debugServInfo serv0 ++ " STARTING ====="
  debugIO . show $ view config serv0
  debugIO . show $ serv0

debugMainFinishing :: (Show a) => Server cl s c a -> IO ()
debugMainFinishing serv' = do
  debugIO $ "===== " ++ debugServInfo serv' ++ " FINISHING ====="
  debugIO . show $ serv'
  debugIO "===== ALL DONE ====="

debugMain :: (Show a) => (Server cl s c a -> IO (Server cl s c a)) -> Server cl s c a -> IO (Server cl s c a)
debugMain mainFn serv0 = do
  debugMainStarting serv0
  serv' <- mainFn serv0
  debugMainFinishing serv'
  return serv'


type ServerWorker cl s c a = (MVar (Server cl s c a)) -> IO ()
type ServerListener cl s c a = Handle -> ServerWorker cl s c a

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
       then debug ("Restaring with new role: " ++ show newRole) exitWith (codeForRole newRole)
       else debugMainFinishing serv' >> exitWith processDone
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

doMaybe :: Monad m => (a -> m ()) -> Maybe a -> m ()
doMaybe = maybe $ return ()


-- Type signature commented out because it needs an explicit FORALL to compile properly and, well, fuck that
-- listenerFromHandle :: Connection conn => (conn -> MVar (Server cl s c a) -> IO ()) -> Handle -> MVar (Server cl s c a) -> IO ()
listenerFromHandle listener hdl servBox = forever $ listener (SimpleHandleConnection hdl) servBox

-- Listen for a message, check for relevance, process it.
-- Including sending a response if necessary.
-- Return the response as well (if any).
listenForSomethingOnce :: (ClientConnection cl a, Persist s, Connection conn, FromJSON a, ToJSON a, Show a) =>
                          (Message -> Bool) -> (Message -> Raft cl s c a (Maybe Message)) ->
                          conn -> MVar (Server cl s c a) -> IO (Maybe Message)
listenForSomethingOnce shouldHandle handler conn servBox = do
  maybeReq <- listen conn
  maybe retryLater maybeProcess maybeReq
    where retryLater :: IO (Maybe Message)
          retryLater = threadDelay kReconnect >> return Nothing

          maybeProcess :: Message -> IO (Maybe Message)
          maybeProcess req
            | shouldHandle req = do
                serv <- takeMVar servBox
                let role0 = view (config.role) serv

                -- Handle the message, send response if any
                let (resp, serv') = runState (handler req) serv
                persist serv'
                putMVar servBox serv'
                doMaybe (`respond` conn) resp

                -- Check if need to step down
                when (view (config.role) serv' /= role0) (raiseSignal sigINT)

                return resp
            | otherwise = debug ("Suppressing unexpected message of type " ++ show (view msgType req)) return Nothing


generateRaftTimeout :: IO NominalDiffTime
generateRaftTimeout = fromRational . toRational . picosecondsToDiffTime . microToPico <$> randomRIO (kTimeoutMin, kTimeoutMax)
  where microToPico :: Int -> Integer
        microToPico = (oneMillion *) . fromIntegral
        oneMillion :: Integer
        oneMillion = 1000000


resetTheTimeout :: MVar UTCTime -> IO ()
resetTheTimeout timeout = addUTCTime <$> generateRaftTimeout <*> getCurrentTime >>= void . swapMVar timeout

-- Used by Follower coordinator thread to listen for requests (only).
listenForRequestOnce :: (ClientConnection cl a, Persist s, Connection conn, FromJSON a, ToJSON a, Show a) =>
                        Maybe (MVar UTCTime) -> conn -> MVar (Server cl s c a) -> IO ()
listenForRequestOnce timeout conn servBox = do
  mResp <- listenForSomethingOnce (isRequest . view msgType) handleRequest conn servBox
  when (doesAbortCandidacy mResp) stepDownIfCandidate
  when (doesResetTimeout mResp) (doMaybe resetTheTimeout timeout)
    where doesResetTimeout :: Maybe Message -> Bool
          doesResetTimeout Nothing = False
          doesResetTimeout (Just msg) = case view msgType msg of
            AppendEntriesResponse -> fromMaybe False (success msg)
            RequestVoteResponse -> fromMaybe False (voteGranted msg)

          doesAbortCandidacy :: Maybe Message -> Bool
          doesAbortCandidacy Nothing = False
          doesAbortCandidacy (Just msg) = view msgType msg == AppendEntriesResponse
                                          && fromMaybe False (success msg)

          stepDownIfCandidate :: IO ()
          stepDownIfCandidate = do  -- TODO: don't block on servBox for this?
            serv <- takeMVar servBox
            case view (config.role) serv of
             Candidate _ -> putMVar servBox (demoteToFollower serv) >> raiseSignal sigINT
             _ -> putMVar servBox serv



-- Used on the Leader and Candidate only, to listen for responses (only).
listenForResponseOnce :: (ClientConnection cl a, Persist s, Connection conn, FromJSON a, ToJSON a, Show a) =>
                         conn -> MVar (Server cl s c a) -> IO ()
listenForResponseOnce conn serv = void $
                                  listenForSomethingOnce (isResponse . view msgType)
                                  (\resp -> handleResponse resp >> return Nothing)
                                  conn serv

--- Main functions for each role

mainForRole :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
               Server cl s c a -> IO (Server cl s c a)
mainForRole serv0 = case view (config.role) serv0 of
  Follower -> followerMain serv0
  Leader -> leaderMain serv0
  (Candidate _) -> candidateMain serv0


newTimeoutTimer :: IO (MVar UTCTime)
newTimeoutTimer = addUTCTime <$> generateRaftTimeout <*> getCurrentTime >>= newMVar

whenTimedOut :: ServerWorker cl s c a -> MVar UTCTime -> ServerWorker cl s c a
whenTimedOut handler nextTimeout servBox = do
  scheduled <- readMVar nextTimeout -- TODO takeMVar?
  timeLeft <- diffUTCTime scheduled <$> getCurrentTime
  if timeLeft > 0
    then threadDelay (secondsToMicrosec timeLeft)
    else do
    resetTheTimeout nextTimeout
    handler servBox

  whenTimedOut handler nextTimeout servBox
    where secondsToMicrosec :: NominalDiffTime -> Microsec
          secondsToMicrosec = round . (1000000 *) . toRational
followerMain :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                Server cl s c a -> IO (Server cl s c a)
followerMain serv0 = debug "Follower listening." newTimeoutTimer >>=
                     \nextTimeout -> withListeners serv0
                                     (listenerFromHandle $ listenForRequestOnce (Just nextTimeout))
                                     [whenTimedOut followerTimeout nextTimeout]
  where followerTimeout :: ServerWorker cl s c a
        followerTimeout servBox = debug "~~~ TIMED OUT [FOLLOWER] ~~~"
                                  $ takeMVar servBox >>= nominateToCandidate >>= putMVar servBox >> raiseSignal sigINT

candidateMain :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                 Server cl s c a -> IO (Server cl s c a) -- TODO: after successful request, downgrade Role
candidateMain serv0 = (,) <$> newIORef 0 <*> newTimeoutTimer
                      >>= \(nextMessageId, nextTimeout) -> -- TODO: need to persist nextMessageId?
                      withListeners serv0
                      (listenerFromHandle $ listenForRequestOnce (Just nextTimeout))
                      (voteForMe nextMessageId
                       : whenTimedOut candidateTimeout nextTimeout
                       : selfListener : cohortListeners (otherCohortIds serv0)
                      )
  where voteForMe nextMessageId servBox = do
          serv <- takeMVar servBox
          if amElected serv
            then promoteToLeader serv >>= putMVar servBox >> raiseSignal sigINT
            else do
              let voteMsg = requestVoteFromCandidate serv
              let self = assertJust "voteForMe :: no SelfConnection" $ view (config.ownFollower) serv
              selfVote <- voteMsg <$> requestInfo nextMessageId serv

              putMVar servBox =<< execState (expectResponsesTo [selfVote]) <$> broadcast nextMessageId voteMsg serv
              respond selfVote $ selfConnection servBox self
              threadDelay kHeartbeat
              voteForMe nextMessageId servBox

        candidateTimeout :: ServerWorker cl s c a
        candidateTimeout servBox = debug "~~~ TIMED OUT [CANDIDATE] ~~~"
                                   $ takeMVar servBox >>= nominateToCandidate >>= putMVar servBox


        amElected :: Server cl s c a -> Bool
        amElected me = case view (config.role) me of
          (Candidate votes) -> 2 * (Map.size . Map.filter (== True) $ votes)
                               > ((+ 1) . Map.size $ view (config.cohorts) me)
--          _ -> error "candidateMain running, but server is not in candidate role." -- TODO
          _ -> debug "candidateMain running, but server is not in candidate role." True

leaderMain :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
              Server cl s c a -> IO (Server cl s c a)
leaderMain serv0 = newIORef 0 >>= \midGen ->
  withListeners serv0
  (listenerFromHandle $ listenForRequestOnce Nothing)
  (broadcastThread midGen
   : heartbeatThread midGen
   : commitReporter
   : selfListener : cohortListeners (otherCohortIds serv0)
  )

-- This thread is responsible for resending unsuccessful AppendEntries requests,
-- and sending heartbeats (empty requests) to caught-up followers to reset their election timeout
heartbeatThread :: (Connection c, ToJSON a) => IORef MessageId -> ServerWorker cl s c a
heartbeatThread midGen servBox = do
  threadDelay kHeartbeat
  serv0 <- takeMVar servBox
  heartbeats <- mapM (finalizeHeartbeat serv0)
                . mapMaybe (customizeHeartbeat serv0)
                . Map.toList . assertJust "heartbeatThread :: no nextIndex" $ view nextIndex serv0
  let serv' = execState (expectResponsesTo $ map snd heartbeats) serv0
  putMVar servBox serv'
  mapM_ (uncurry $ flip respond) heartbeats
  heartbeatThread midGen servBox
  where entriesToRetry :: Server cl s c a -> LogIndex -> [IndexedEntry a]  -- TODO this +1 feels kinda hacky
        entriesToRetry serv nxtI = mapMaybe (\i -> (,) <$> Just i <*> entry i (view log serv)) [nxtI .. viewLastLogIndex serv]
        customizeHeartbeat :: ToJSON a => Server cl s c a -> (ServerId, LogIndex) -> Maybe (c, BaseMessage)
        customizeHeartbeat serv (fid,nxtI) = (,)
                                           <$> Map.lookup fid (view (config.cohorts) serv)
                                           <*> Just (appendEntriesFromLeader serv $ entriesToRetry serv nxtI)
        finalizeHeartbeat :: Server cl s c a -> (c, BaseMessage) -> IO (c, Message)
        finalizeHeartbeat serv (c, base) = (,) <$> return c <*> (base <$> requestInfo midGen serv)


commitReporter :: (ClientConnection cl a, Show a) => ServerWorker cl s c a
commitReporter servBox = do
  serv <- takeMVar servBox
  let latestCommitted = view entryData <$> view commitIndex serv `entry` view log serv
  doMaybe (`committed` view (config.client) serv) latestCommitted
  putMVar servBox serv
  threadDelay kCommitReport
  commitReporter servBox

selfListener :: (ClientConnection cl a, Persist s, FromJSON a, ToJSON a, Show a) =>
                ServerWorker cl s c a
selfListener servBox = do
  serv <- takeMVar servBox
  let self = assertJust "selfListener :: No SelfConnection to listen to"
             $ view (config.ownFollower) serv
  putMVar servBox serv
  listenForResponseOnce (selfConnection servBox self) servBox
  selfListener servBox


cohortListeners :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                   [ServerId] -> [ServerWorker cl s c a]
cohortListeners = map listenForResponsesFrom
  where listenForResponsesFrom sid servBox = do
          serv <- takeMVar servBox
          let conn = assertJust "cohortListeners :: unknown ServerId"
                     $ sid `Map.lookup` view (config.cohorts) serv
          putMVar servBox serv
          listenForResponseOnce conn servBox
          listenForResponsesFrom sid servBox


broadcastThread :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
                   IORef MessageId -> MVar (Server cl s c a) -> IO ()
broadcastThread nextMessageId servBox = do
  serv0 <- takeMVar servBox
  let catchUp = appendEntriesFromLeader serv0 . logWithIndices $ serv0
  serv' <- broadcast nextMessageId catchUp serv0
  putMVar servBox serv'
  leaderLoop nextMessageId servBox

leaderLoop :: (ClientConnection cl a, Persist s, Connection c, ToJSON a, FromJSON a, Show a) =>
              IORef MessageId -> MVar (Server cl s c a) -> IO ()
leaderLoop nextMid servBox = do
  nextLogEntry <- getLogEntry =<< view (config.client) <$> readMVar servBox
  serv0 <- takeMVar servBox

  let serv = over log (logMap (++ [LogEntry (view currentTerm serv) nextLogEntry])) serv0
  let update = appendEntriesFromLeader serv [last . logWithIndices $ serv]
  let self = assertJust "leaderLoop :: no SelfConnection" $ view (config.ownFollower) serv
  selfUpdate <- update <$> requestInfo nextMid serv

  serv' <- execState (expectResponsesTo [selfUpdate]) <$> broadcast nextMid update serv
  putMVar servBox serv'
  respond selfUpdate $ selfConnection servBox self

  leaderLoop nextMid servBox


main :: IO ()
main = do
  args <- getArgs

  case args of
   (myId:rest) -> do

     let sid = fromIntegral . read $ myId :: ServerId
--     conf <- readJSONConfig (kConfigFile sid) sid :: IO (ServerConfig SimpleIncrementingClient JsonStorage HandleConnection String)
     conf <- readJSONConfig (kConfigFile sid) sid :: IO (ServerConfig GetlineClient JsonStorage HandleConnection Int)
     serv <- fromPersist . initializeFollower $ conf
     case rest of
      ("test":_) -> testMain sid

      ("log":_) -> logMain sid $ view log serv

      ("leader":_) -> void $ leaderMain =<< promoteToLeader serv

      ("candidate":_) -> void $ candidateMain =<< nominateToCandidate serv

      ("follower":_) -> defaultToFollower serv

      [] -> defaultToFollower serv

      _ -> error "Invalid argument: startup command not found."

   _ -> error "Invalid argument: server id required."
  where defaultToFollower = void . followerMain

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
    serv <- newIORef . initializeFollower $ ServerConfig Follower conf Nothing Map.empty (fromName storageName) AbortClient
    return $ InMemoryConnection serv mid
    where storageName = kLogDir ++ host ++ "_" ++ show port ++ ".local.json"

instance Show a => Show (InMemoryConnection cl s c a) where
         show (InMemoryConnection servRef _) = show . unsafePerformIO . readIORef $ servRef

simpleConfig :: ClusterConfig
simpleConfig = ClusterConfig {
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
  serv <- promoteToLeader $ initializeFollower conf
  case view role conf of
   Leader -> leaderMain . injectPersistentState (1, Just myId, lg) $ serv


writeTestConfig :: ClusterConfig -> IO ()
writeTestConfig = ByteString.writeFile (kConfigDir ++ "config.auto.json") . encode

readTestConfig :: IO (Maybe ClusterConfig)
readTestConfig = decode <$> ByteString.readFile (kConfigDir ++ "config.auto.json")
