{-# LANGUAGE RecordWildCards #-}

module Network.Wai.Handler.Warp.HTTP2.Timeout (
    withTimer
  ) where

import qualified Control.Exception as E
import Control.Reaper
import qualified Data.Foldable as F
import Data.IORef (readIORef, writeIORef)
import Data.IntMap.Strict as M
import Network.Wai.Handler.Warp.HTTP2.Types
import System.IO.Unsafe

initialize :: Int -> IO StreamTable
initialize timeout = mkReaper defaultReaperSettings
    { reaperAction = clean
    , reaperDelay = timeout
    , reaperCons = uncurry M.insert
    , reaperNull = M.null
    , reaperEmpty = empty
    }

clean :: IntMap Stream -> IO (IntMap Stream -> IntMap Stream)
clean old = do
    let (inactive, active) = partition stacked old
    F.mapM_ gonext inactive
    F.mapM_ inactivate active
    return $ union active
  where
    stacked Stream{..} = unsafePerformIO $ do
        x <- readIORef streamActivity
        return $ x == Inactive
    gonext Stream{..} = do
        act <- readIORef streamTimeoutAction
        act
    inactivate Stream{..} = writeIORef streamActivity Inactive

withTimer :: Int -> (StreamTable -> IO a) -> IO a
withTimer timeout action = E.bracket (initialize timeout)
                                     reaperStop
                                     action
