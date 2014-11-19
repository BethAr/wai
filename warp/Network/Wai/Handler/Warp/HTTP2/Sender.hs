{-# LANGUAGE RecordWildCards #-}

module Network.Wai.Handler.Warp.HTTP2.Sender where

import Control.Concurrent (putMVar)
import Control.Concurrent.STM
import qualified Control.Exception as E
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Network.HTTP2
import Network.Wai.Handler.Warp.HTTP2.Types
import qualified Network.Wai.Handler.Warp.Timeout as T
import Network.Wai.Handler.Warp.Types

----------------------------------------------------------------

goawayFrame :: StreamIdentifier -> ErrorCodeId -> ByteString -> ByteString
goawayFrame sid etype debugmsg = encodeFrame einfo frame
  where
    einfo = encodeInfo id 0
    frame = GoAwayFrame sid etype debugmsg

resetFrame :: ErrorCodeId -> StreamIdentifier -> ByteString
resetFrame etype sid = encodeFrame einfo frame
  where
    einfo = encodeInfo id $ fromStreamIdentifier sid
    frame = RSTStreamFrame etype

settingsFrame :: (FrameFlags -> FrameFlags) -> SettingsList -> ByteString
settingsFrame func alist = encodeFrame einfo $ SettingsFrame alist
  where
    einfo = encodeInfo func 0

pingFrame :: ByteString -> ByteString
pingFrame bs = encodeFrame einfo $ PingFrame bs
  where
    einfo = encodeInfo setAck 0

----------------------------------------------------------------

-- fixme: packing bytestrings
frameSender :: Connection -> InternalInfo -> Context -> IO ()
frameSender Connection{..} InternalInfo{..} Context{..} =
    loop `E.finally` putMVar wait ()
  where
    loop = do
        cont <- readQ >>= send
        T.tickle threadHandle
        when cont loop
    readQ = atomically $ readTQueue outputQ
    send bs
      -- "" is EOF. Even if payload is "", its header is NOT "".
      | BS.length bs == 0 = return False
      | otherwise         = do
        -- fixme: connSendMany
        connSendAll bs
        return True
