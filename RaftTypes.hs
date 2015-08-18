{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module RaftTypes where
import qualified Prelude (log)
import Prelude hiding (log)
import Data.Map as Map
import GHC.Generics
import Control.Lens
import Data.Aeson
import Control.Monad

data Role = Leader | Follower | Candidate

newtype MessageId = MessageId Integer
                  deriving (Eq, Ord, Show, Num, Enum, Generic)
newtype Term = Term Integer
             deriving (Eq, Ord, Show, Num, Enum, Generic)
newtype ServerId = ServerId Integer
                 deriving (Eq, Ord, Show, Num, Enum, Generic)
newtype LogIndex = LogIndex Integer
                 deriving (Eq, Ord, Show, Num, Enum, Generic)

data LogEntry a = LogEntry Term a
                  deriving (Show, Generic)

type Log a = [LogEntry a]

instance ToJSON Term
instance FromJSON Term
instance ToJSON ServerId
instance FromJSON ServerId
instance ToJSON LogIndex
instance FromJSON LogIndex
instance ToJSON a => ToJSON (LogEntry a)
instance FromJSON a => FromJSON (LogEntry a)
-- instance ToJSON a => ToJSON (Log a)
-- instance FromJSON a => FromJSON (Log a)

type ServerMap a = Map ServerId a
data ServerConfig s c e = ServerConfig {
  _cohorts :: ServerMap c,
  _storage :: s e
  }
makeLenses ''ServerConfig

data Server s c e = Server {
  --- Raft State
  -- Follower state
  _currentTerm :: Term,
  _votedFor :: Maybe ServerId,
  _log :: Log e,
  _commitIndex :: LogIndex,
  _lastApplied :: LogIndex,
  -- Leader-only state
  _nextIndex :: Maybe (ServerMap LogIndex),
  _matchIndex :: Maybe (ServerMap LogIndex),

  --- Non-raft state
  _config :: ServerConfig s c e
  }
makeLenses ''Server


type PersistentState a = (Term, Maybe ServerId, Log a)

injectPersistentState :: Server s c e -> PersistentState e -> Server s c e
injectPersistentState serv (t, v, l) = set currentTerm t . set votedFor v . set log l $ serv

extractPersistentState :: Server s c e -> PersistentState e
extractPersistentState serv = (view currentTerm serv, view votedFor serv, view log serv)

class Persist s where
  writeToStable :: ToJSON a => PersistentState a -> s a -> IO ()
  readFromStable :: FromJSON a => s a -> IO (PersistentState a)

persist :: (Persist s, ToJSON e) => Server s c e -> IO ()
persist serv = writeToStable (extractPersistentState serv) $ view (config.storage) serv

fromPersist :: (Persist s, FromJSON e) => Server s c e -> IO (Server s c e)
fromPersist serv = readFromStable (view (config.storage) serv) >>= return . injectPersistentState serv

-- main :: IO()
-- main = do
--   let r = AppendEntriesResult 0 True
--   print $ view aer_term r
