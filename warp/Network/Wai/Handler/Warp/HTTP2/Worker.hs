{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Wai.Handler.Warp.HTTP2.Worker where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception as E
import Control.Monad (void, forever)
import Data.Typeable
import Network.Wai
import Network.Wai.Handler.Warp.HTTP2.Response
import Network.Wai.Handler.Warp.HTTP2.Types
import Network.Wai.Handler.Warp.IORef

data Break = Break deriving (Show, Typeable)

instance Exception Break

-- fixme: tickle activity
-- fixme: sending a reset frame?
worker :: Context -> Application -> EnqRsp -> IO ()
worker Context{..} app enQResponse = go `E.catch` gonext
  where
    go = forever $ do
        Input strm@Stream{..} req <- atomically $ readTQueue inputQ
        tid <- myThreadId
        E.bracket (writeIORef streamTimeoutAction $ E.throwTo tid Break)
                  (\_ -> writeIORef streamTimeoutAction $ return ())
                  $ \_ -> do
                      void $ app req $ enQResponse strm
                      writeIORef streamState Closed
    gonext Break = go `E.catch` gonext

