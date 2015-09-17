{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Demo where

import Control.Monad
import Data.IORef
import System.IO

import ConnectionTypes

data GetlineClient a = GetlineClient {
  latestValue :: IORef (Maybe a)
  }

newGetlineClient :: IO (GetlineClient a)
newGetlineClient = GetlineClient <$> newIORef Nothing

instance (Read a, Eq a) => ClientConnection GetlineClient a where
  fromClientConfig _ = newGetlineClient

  getLogEntry _ = putStr "> " >> hFlush stdout >> (read <$> getLine)

  committed val cli = do
    oldStored <- atomicModifyIORef' (latestValue cli) (\old -> (Just val, old))
    when (oldStored /= Just val) $ putStrLn ("\r!! COMMITTED: " ++ show val) >> putStr "> " >> hFlush stdout
