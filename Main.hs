module Main where

import Control.Concurrent
import System.Environment
import System.Exit
import System.Posix

import qualified Raft
import ExitCodes

main :: IO ()
main = do
  args <- getArgs
  case args of
   [myId] -> newEmptyMVar >>= \interrupted -> runAs interrupted (read myId) "follower"
   _ -> error "Invalid arguments. Expected one arg: `raft SERVER_ID`"

announceDone :: ExitCode -> IO ()
announceDone code = putStrLn $ "Raft Supervisor :: shutting down. Raft return status: " ++ show code

runAs :: MVar () -> Int -> String -> IO ()
runAs interrupted sid role  = do
  pid <- forkProcess (withArgs [show sid, role] Raft.main)
  installHandler keyboardSignal (Catch $ fwdSignal interrupted pid) Nothing
  status <- getProcessStatus True False pid
  wasInterrupted <- tryTakeMVar interrupted
  case (wasInterrupted, status) of
   (Just _, Just (Exited code)) -> announceDone code
   (Nothing, Just (Exited code)) -> maybe (announceDone code) (runAs interrupted sid) (argForRole <$> roleForCode code)
   (_, Just err) -> putStrLn $ "Raft Supervisor :: shutting down with error: " ++ show err
   _ -> putStrLn "Raft Supervisor :: shutting down with unknown error."

fwdSignal :: MVar () -> ProcessID -> IO ()
fwdSignal interrupt pid = do
  putMVar interrupt ()
  signalProcess keyboardSignal pid
