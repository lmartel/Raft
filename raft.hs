{-# LANGUAGE NoImplicitPrelude #-}
module Raft where
import qualified Prelude (log)
import Prelude hiding (log)
import Control.Lens
import Control.Monad
import Control.Monad.State
import qualified Data.Map as Map

import RaftTypes
import MessageTypes
import ConnectionTypes
import JsonStorage

initializeFollower :: ServerConfig s c a -> Server s c a
initializeFollower conf = Server {
  _currentTerm = Term 0,
  _votedFor = Nothing,
  _log = [],
  _commitIndex = LogIndex 0,
  _lastApplied = LogIndex 0,
  _nextIndex = Nothing,
  _matchIndex = Nothing,

  _config = conf
  }

promoteToLeader :: Server s c a -> Server s c a
promoteToLeader base = set matchIndex (Just mis) . set nextIndex (Just nis) $ base
  where nis :: ServerMap LogIndex
        nis = Map.map (\_ -> viewLastLogIndex base) . view (config.cohorts) $ base
        mis :: ServerMap LogIndex
        mis = Map.map (\_ -> LogIndex 0) . view (config.cohorts) $ base

viewLastLogIndex :: Server s c a -> LogIndex
viewLastLogIndex = LogIndex . fromIntegral . length . view log

logWithIndices :: Server s c a -> [(LogIndex, LogEntry a)]
logWithIndices = zip [1..] . view log

serverCohorts :: Server s c a -> [c]
serverCohorts = map snd . Map.toList . view (config.cohorts)

-- TODO switch to State monad for tracking next id, put MessageId back into Message type explicitly
type RPC' a = State MessageId a

prepareBroadcast :: RPC Message -> Server s c a -> [(c, RPC Message)]
prepareBroadcast msg serv = zip (serverCohorts serv) $ iterate (>> NextRequest (payload msg)) msg

broadcast :: Connection c => [(c, RPC Message)] -> [IO (Maybe (RPC Message))]
broadcast = map (\(c, m) -> request m c)

main :: IO ()
main = do
  let me = promoteToLeader $ initializeFollower conf
  let bs = prepareBroadcast (pure append) me
  let results = broadcast bs
  result <- sequence results
  print result
    where conf = ServerConfig followers (JsonStorage "test.json")
          followers = Map.fromList [(1, FakeConnection),
                                (2, FakePartition),
                                (3, FakeConnection),
                                (4, FakePartition)
                               ]
          append :: Message
          append = appendEntries 11 0 1 2 (zip [1..] myLog) 3
          myLog = [] :: [LogEntry String]
