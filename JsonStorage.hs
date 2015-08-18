{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module JsonStorage where
import System.IO
import Data.Aeson
import Control.Monad
import qualified Data.ByteString.Lazy as B

import RaftTypes

data JsonStorage a = JsonStorage String

filename :: JsonStorage a -> String
filename (JsonStorage s) = s

instance ToJSON a => ToJSON (PersistentState a)
instance FromJSON a => FromJSON (PersistentState a)

instance Persist JsonStorage where
  writeToStable = writeToJson
  readFromStable = readFromJson

writeToJson :: ToJSON a => PersistentState a -> JsonStorage a -> IO ()
writeToJson state stor = B.writeFile (filename stor) (encode state)

readFromJson :: FromJSON a => JsonStorage a -> IO (PersistentState a)
readFromJson stor = B.readFile (filename stor) >>= (\str -> case decode str of
                                                     Nothing -> error $ "cannot read or parse " ++ filename stor
                                                     -- TODO retry but backoff intelligently
                                                     (Just state') -> return state')


-- main :: IO ()
--main = do
  --let stor = JsonStorage "test.json"
  -- let st = (1, Just 0, [LogEntry 1 "test"]) :: PersistentState String
  -- writeToStable st stor
  -- st' <- readFromStable stor :: IO (PersistentState String)
  -- putStrLn $ showSt st'
  --   where showSt (t, sid, l) = show t ++ show sid ++ show l
