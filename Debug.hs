module Debug where
import Data.IORef
import System.IO
import System.IO.Unsafe

assertJust :: String -> Maybe a -> a
assertJust _ (Just x) = x
assertJust err Nothing = error err

-- Global mutable variables in haskell, wheee
-- This isn't too scary as a single IORef that's only used in dev for debugging.
-- (but I still wish there were a sound-er way to do this)
{-# NOINLINE debugTarget #-}
debugTarget :: IORef Handle
debugTarget = unsafePerformIO $ newIORef stdout


debugIO' :: String -> IO ()
-- debugIO' _ = return () -- Disable debug log globally
debugIO' err = do
  target <- readIORef debugTarget
  hPutStr target err
  hFlush target

debugIO :: String -> IO ()
debugIO err = debugIO' (err ++ "\n")

{-# NOINLINE debug' #-}
debug' :: String -> a -> a
debug' err  = (unsafePerformIO (debugIO' err) `seq`)


{-# NOINLINE debug #-}
debug :: String -> a -> a
debug err = debug' (err ++ "\n")

-- Debug helpers

{-# NOINLINE debugIf #-}
debugIf :: String -> Bool -> Bool
debugIf err True = debug' err True
debugIf _ False = False

{-# NOINLINE debugUnless #-}
debugUnless :: String -> Bool -> Bool
debugUnless err = debugIf err . not

{-# NOINLINE debugUnlessM #-}
debugUnlessM :: String -> Maybe Bool -> Maybe Bool
debugUnlessM _ (Just True) = Just True
debugUnlessM err mb = debug' err mb

{-# NOINLINE debugPrint #-}
debugPrint :: Show a => a -> a
debugPrint dat = debug' (show dat) dat
