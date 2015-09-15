{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
module MessageTypes where
import qualified Prelude (log)
import Prelude hiding (log)
import Data.Map as Map
import GHC.Generics
import Control.Lens
import Data.Aeson
import Control.Monad
import Data.Maybe
import Data.List
import Data.ByteString.Lazy.Internal (ByteString)

import RaftTypes

extract :: FromJSON a => String -> Message -> Maybe a
extract name msg = find (\(s, _) -> s == name) (view msgArgs msg) >>= (decode . rawArg . snd)

kTerm = "term"
kLeaderId = "leaderId"
kPrevLogIndex = "prevLogIndex"
kPrevLogTerm = "prevLogTerm"
kEntries = "entries"
kLeaderCommit = "leaderCommit"

kSuccess = "success"

kCandidateId = "candidateId"
kLastLogIndex = "lastLogIndex"
kLastLogTerm = "lastLogTerm"

kVoteGranted = "voteGranted"


term :: Message -> Maybe Term
term = extract kTerm
leaderId :: Message -> Maybe ServerId
leaderId = extract kLeaderId
prevLogIndex :: Message -> Maybe LogIndex
prevLogIndex = extract kPrevLogIndex
prevLogTerm :: Message -> Maybe Term
prevLogTerm = extract kPrevLogTerm
entries :: FromJSON e => Message -> Maybe [(LogIndex, LogEntry e)]
entries = extract kEntries
leaderCommit :: Message -> Maybe LogIndex
leaderCommit = extract kLeaderCommit

success :: Message -> Maybe Bool
success = extract kSuccess

candidateId :: Message -> Maybe ServerId
candidateId = extract kCandidateId
lastLogIndex :: Message -> Maybe LogIndex
lastLogIndex = extract kLastLogIndex
lastLogTerm :: Message -> Maybe Term
lastLogTerm = extract kLastLogTerm

voteGranted :: Message -> Maybe Bool
voteGranted = extract kVoteGranted

heartbeatFromLeader :: ToJSON a => Server cl s c a -> BaseMessage
heartbeatFromLeader = flip appendEntriesFromLeader []

appendEntriesFromLeader :: ToJSON a => Server cl s c a -> [(LogIndex, LogEntry a)] -> BaseMessage
appendEntriesFromLeader s es = appendEntries (view currentTerm s) (view serverId s) findPrevLogIndex findPrevLogTerm es (view commitIndex s)
  where findPrevLogTerm :: Term
        findPrevLogTerm = case termAtIndex findPrevLogIndex s of
                           Nothing -> error "appendEntriesFromLeader :: gap in log!"
                           (Just t) -> t

        findPrevLogIndex :: LogIndex
        findPrevLogIndex = case es of
          [] -> viewLastLogIndex s
          (e:_) -> fst e - 1

encodeArg :: ToJSON a => a -> EncodedArg
encodeArg = EncodedArg . encode

appendEntries :: ToJSON a => Term -> ServerId -> LogIndex -> Term -> [(LogIndex, LogEntry a)] -> LogIndex -> BaseMessage
appendEntries t lid pli plt es lc = Message AppendEntries [
  (kTerm, encodeArg t),
  (kLeaderId, encodeArg lid),
  (kPrevLogIndex, encodeArg pli),
  (kPrevLogTerm, encodeArg plt),
  (kEntries, encodeArg es),
  (kLeaderCommit, encodeArg lc)
  ]

appendEntriesResponse :: Term -> Bool -> BaseMessage
appendEntriesResponse t s = Message AppendEntriesResponse [
  (kTerm, encodeArg t),
  (kSuccess, encodeArg s)
  ]

requestVoteFromCandidate :: Server cl s c a -> BaseMessage
requestVoteFromCandidate s = requestVote (view currentTerm s) (view serverId s) (viewLastLogIndex s) (viewLastLogTerm s)

requestVote :: Term -> ServerId -> LogIndex -> Term -> BaseMessage
requestVote t cid lli llt = Message RequestVote [
  (kTerm, encodeArg t),
  (kCandidateId, encodeArg cid),
  (kLastLogIndex, encodeArg lli),
  (kLastLogTerm, encodeArg llt)
  ]

requestVoteResponse :: Term -> Bool -> BaseMessage
requestVoteResponse t vg = Message RequestVoteResponse [
  (kTerm, encodeArg t),
  (kVoteGranted, encodeArg vg)
  ]

-- data AppendEntries e = AppendEntries {
--   _ae_term :: Term,
--   _leaderId :: ServerId,
--   _prevLogIndex :: LogIndex,
--   _prevLogTerm :: Term,
--   _entries :: [(LogIndex, LogEntry e)],
--   _leaderCommit :: LogIndex
--   }

-- data AppendEntriesResult = AppendEntriesResult {
--   _aer_term :: Term,
--   _success :: Bool
--   }
-- makeLenses ''AppendEntriesResult

-- data RequestVote = RequestVote {
--   _rv_term :: Term,
--   _candidateId :: ServerId,
--   _lastLogIndex :: LogIndex,
--   _lastLogTerm :: Term
--   }
-- makeLenses ''RequestVote

-- data RequestVoteResult = RequestVoteResult {
--   _rvr_term :: Term,
--   _voteGranted :: Bool
--   }
-- makeLenses ''RequestVoteResult

-- main :: IO()
-- main = do
--   let m = pure $ appendEntriesResponse 6 True :: RPC Message
--   let m' = m >>= NextResponse m >> NextRequest m >> NextResponse m
--   print m'
