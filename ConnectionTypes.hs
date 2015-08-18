module ConnectionTypes where

import RaftTypes
import MessageTypes

class Connection c where
  request :: RPC Message -> c -> IO (Maybe (RPC Message))
  respond :: RPC Message -> c -> IO ()

data FakeConnection = FakeConnection | FakePartition
                    deriving Show

instance Connection FakeConnection where
  request rpc FakeConnection = case rpcType rpc of
    AppendEntries -> pure . Just $ rpc >> NextResponse (appendEntriesResponse 7 True)
    RequestVote -> pure . Just $ rpc >> NextResponse (requestVoteResponse 9 False)
  request _ FakePartition = pure Nothing

  respond _ _ = pure ()
