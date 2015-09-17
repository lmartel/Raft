{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Demo where

import Control.Monad
import Data.IORef
import System.IO

import ConnectionTypes

data GetlineClient a = GetlineClient {
  gl_latestValue :: IORef (Maybe a)
  }

demoReportVal :: Show a => a -> IO ()
demoReportVal new = putStrLn $ "\r!! CURRENT COMMITTED VALUE: " ++ show new

instance (Read a, Eq a, Show a) => ClientConnection GetlineClient a where
  fromClientConfig _ = GetlineClient <$> newIORef Nothing

  getLogEntry cl@(GetlineClient stor) = do
    putStr "> "
    hFlush stdout
    ln <- getLine
    if null ln
      then do
      prev <- readIORef stor
      maybe (return ()) demoReportVal prev
      getLogEntry cl
      else return $ read ln

  committed val cli = do
    oldStored <- atomicModifyIORef' (gl_latestValue cli) (\old -> (Just val, old))
    when (oldStored /= Just val) $ demoReportVal val >> putStr "> " >> hFlush stdout

data ReportOnlyClient a = ReportOnlyClient {
  ro_latestValue :: IORef (Maybe a)
  }

instance (Eq a, Show a) => ClientConnection ReportOnlyClient a where
  fromClientConfig _ = ReportOnlyClient <$> newIORef Nothing

  getLogEntry _ = error "ERROR! ReportOnlyClient should not receive commits."

  committed val cli = do
    oldStored <- atomicModifyIORef' (ro_latestValue cli) (\old -> (Just val, old))
    when (oldStored /= Just val) $ demoReportVal val
