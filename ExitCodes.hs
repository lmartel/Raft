module ExitCodes where

import qualified Data.Map as Map
import System.Exit

import RaftTypes

processDone :: ExitCode
processDone = ExitSuccess

unknownError :: ExitCode
unknownError = ExitFailure 1

switchToLeader :: ExitCode
switchToLeader = ExitFailure 2

switchToCandidate :: ExitCode
switchToCandidate = ExitFailure 3

switchToFollower :: ExitCode
switchToFollower = ExitFailure 4

codeForRole :: Role -> ExitCode
codeForRole Leader = switchToLeader
codeForRole Follower = switchToFollower
codeForRole (Candidate _) = switchToCandidate

argForRole :: Role -> String
argForRole Leader = "leader"
argForRole Follower = "follower"
argForRole (Candidate _) = "candidate"

roleForCode :: ExitCode -> Maybe Role
roleForCode code
  | code == switchToLeader    = Just Leader
  | code == switchToCandidate = Just $ Candidate Map.empty
  | code == switchToFollower  = Just Follower
  | otherwise                 = Nothing
