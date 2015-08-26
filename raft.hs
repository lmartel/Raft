{-# LANGUAGE NoImplicitPrelude #-}
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
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as ByteString

import RaftTypes
import MessageTypes
import ConnectionTypes
import JsonStorage

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

  _config = set role Follower conf
  }

promoteToLeader :: Server s c a -> Server s c a
promoteToLeader base = set (config.role) Leader . set nextIndex (Just nis) . set matchIndex (Just mis) $ base
  where nis :: ServerMap LogIndex
        nis = Map.map (\_ -> viewLastLogIndex base) . view (config.cohorts) $ base
        mis :: ServerMap LogIndex
        mis = Map.map (\_ -> LogIndex 0) . view (config.cohorts) $ base

demoteToFollower :: Server s c a -> Server s c a
demoteToFollower = set (config.role) Follower . set nextIndex Nothing . set matchIndex Nothing

--- Accessors and helpers

lastIndex :: Log a -> LogIndex
lastIndex = LogIndex . fromIntegral . length . view logEntries

viewLastLogIndex :: Server s c a -> LogIndex
viewLastLogIndex = lastIndex . view log

withIndices :: Log a -> [(LogIndex, LogEntry a)]
withIndices = zip [1..] . view logEntries

logWithIndices :: Server s c a -> [(LogIndex, LogEntry a)]
logWithIndices = withIndices . view log

serverCohorts :: Server s c a -> [c]
serverCohorts = map snd . Map.toList . view (config.cohorts)

--- Sending/receiving messages

requestInfo :: IORef MessageId -> Server s c a -> IO MessageInfo
requestInfo midRef serv = readIORef midRef >>= writeNextMid
  where writeNextMid mid = atomicWriteIORef midRef (mid + 1) >> return (MessageInfo (view (config.serverId) serv) mid)

responseInfo :: Message -> Server s c a -> MessageInfo
responseInfo req serv = MessageInfo (view (config.serverId) serv) (view (info.msgId) req)

prepareBroadcast :: IORef MessageId -> BaseMessage -> Server s c a -> IO [(c, Message)]
prepareBroadcast midRef msg serv = readIORef midRef >>= writeNextMid . requests
  where writeNextMid [] = return []
        writeNextMid reqs = atomicWriteIORef midRef (nextMid reqs) >> return reqs
        nextMid :: [(c, Message)] -> MessageId
        nextMid reqs = (1 + ) . last $ map (view (info.msgId) . snd) reqs
        requests mid1 = zipWith prepareMessage (serverCohorts serv) [mid1..]
        prepareMessage c mid = (c, msg $ MessageInfo (view (config.serverId) serv) mid)

sendRequest :: Connection c => (c, Message) -> IO (Maybe Message)
sendRequest (c,m) = request m c

broadcast :: Connection c => [(c, Message)] -> IO [Maybe Message]
broadcast = mapM (\(c, m) -> request m c)

broadcastUntil :: Connection c => ([Message] -> Bool) -> [(c, Message)] -> IO [Message]
broadcastUntil fn = broadcastUntil' . map Right
  where broadcastUntil' :: Connection c => [Either Message (c, Message)] -> IO [Message]
        broadcastUntil' reqs = broadcast' reqs >>= retryIfNeeded (rights reqs)
        broadcast' :: Connection c => [Either Message (c, Message)] -> IO [Maybe Message]
        broadcast' = mapM (\e -> case e of
                                  (Left msg) -> return . Just $ msg
                                  (Right req) -> sendRequest req
                          )

        retryIfNeeded :: Connection c => [(c, Message)] -> [Maybe Message] -> IO [Message]
        retryIfNeeded reqs responses = if fn $ catMaybes responses
                                  then return $ catMaybes responses
                                  else broadcastUntil' $ zipWith retrySome reqs responses
        retrySome :: Connection c => (c, Message) -> Maybe Message -> Either Message (c, Message)
        retrySome req Nothing = Right req
        retrySome _ (Just res) = Left res

--- Raft algorithm core

handleRPC :: FromJSON a => Message -> Raft s c a Message
handleRPC msg = case view msgType msg of
  AppendEntries -> processAppendEntries msg
  _ -> error "handleRPC :: Handler not yet implemented."

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
        response b me = appendEntriesResponse (view currentTerm me) b $ view info msg

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

kConfigFile :: String
kConfigFile = "config.json"
kLogFile :: String
kLogFile = "log.json"

configureCohorts :: Connection c => ClusterConfig -> ServerMap c
configureCohorts = Map.fromList . map (\(sid, h, p) -> (sid, fromHostPort h p)) . view servers

configureSelf :: Connection c => ServerId -> ClusterConfig -> (s a -> ServerConfig s c a)
configureSelf myId cluster = ServerConfig myId myRole (configureCohorts cluster)
  where myRole = if myId == view leader cluster
                 then Leader
                 else Follower

-- TODO: generalize storage / log entry type (use reflection?)
readJSONConfig :: Connection c => String -> ServerId -> IO (ServerConfig JsonStorage c String)
readJSONConfig f myId = ByteString.readFile f >>= return . \confStr -> case decode confStr of
                              Nothing -> error $ "cannot read or parse config file: " ++ f
                              (Just conf) -> configureSelf myId conf (JsonStorage kLogFile)


serverMain :: ServerId -> IO ()
serverMain myId = do
  conf <- readJSONConfig kConfigFile myId :: IO (ServerConfig JsonStorage FakeConnection String)
  case view role conf of
   Leader -> leaderMain conf
   Follower -> followerMain conf

singleThreadedFollowerMain :: Connection c => ServerConfig s c a -> IO ()
singleThreadedFollowerMain = undefined

followerMain :: Connection c => ServerConfig s c a -> IO ()
followerMain = undefined

leaderMain :: Connection c => ServerConfig s c a -> IO ()
leaderMain = undefined

singleThreadedLeaderMain :: IO ()
singleThreadedLeaderMain = undefined


main :: IO ()
main = do
  args <- getArgs
  case args of
   [] -> testMain
   (myId:_) -> serverMain . fromIntegral $ read myId
   _ -> error "Invalid arguments." -- TODO fancier arg parsing / flags


-- Testing and testing utils

-- TODO store server state in connection, probably using IORef
data SelfConnection s c a = SelfConnection (Server s c a)

instance Connection (SelfConnection s c a) where
  request msg (SelfConnection serv) = (\(resp, serv') ->

                                        )
                                      $ runState (handleRPC msg) serv

testMain :: IO ()
testMain = do
  nextMessageId <- newIORef 1

  print "Test: sending messages."
  let me = promoteToLeader $ initializeFollower conf
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

  print "All done!"

    where conf = ServerConfig 0 Booting followers (JsonStorage "test.json")
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
