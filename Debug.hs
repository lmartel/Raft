module Debug where
import System.IO
import System.IO.Unsafe

{-# NOINLINE debug #-}
debug :: String -> a -> a
debug err dat = unsafePerformIO (putStrLn err) `seq` dat
-- debug _ = id

{-# NOINLINE debug' #-}
debug' :: String -> a -> a
debug' err dat = unsafePerformIO (putStr err >> hFlush stdout) `seq` dat
-- debug' _ = id


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
