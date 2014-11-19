{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Wai.Handler.Warp.HTTP2.Response (enqueueRsp, EnqRsp) where

import Blaze.ByteString.Builder
import Control.Arrow (first)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import Data.CaseInsensitive (foldedCase)
import Data.IORef (readIORef, writeIORef)
import Network.HPACK
import qualified Network.HTTP.Types as H
import Network.HTTP2
import Network.Wai
import Network.Wai.Handler.Warp.HTTP2.Types
import Network.Wai.Handler.Warp.Header
import Network.Wai.Handler.Warp.Response
import qualified Network.Wai.Handler.Warp.Settings as S (Settings, settingsServerName)
import Network.Wai.Handler.Warp.Types
import Network.Wai.Internal (Response(..), ResponseReceived(..))
import System.IO (withFile, IOMode(..))

----------------------------------------------------------------

{-
ResponseFile Status ResponseHeaders FilePath (Maybe FilePart)
ResponseBuilder Status ResponseHeaders Builder
ResponseStream Status ResponseHeaders StreamingBody
ResponseRaw (IO ByteString -> (ByteString -> IO ()) -> IO ()) Response
-}

-- enqueueRsp :: TQueue Rsp -> Int -> Response -> IO ResponseReceived

type EnqRsp = Stream -> Response -> IO ResponseReceived

-- fixme: more efficient buffer handling
enqueueRsp :: Context -> InternalInfo -> S.Settings -> EnqRsp
enqueueRsp ctx@Context{..} ii settings Stream{..} (ResponseBuilder st hdr0 bb) = do
    hdrframe <- headerFrame ctx ii settings streamNumber st hdr0
    atomically $ writeTQueue outputQ hdrframe
    atomically $ writeTQueue outputQ datframe
    return ResponseReceived
  where
    einfo = encodeInfo setEndStream streamNumber
    datframe = encodeFrame einfo $ DataFrame $ toByteString bb

-- fixme: filepart
enqueueRsp ctx@Context{..} ii settings Stream{..} (ResponseFile st hdr0 file _) = do
    hdrframe <- headerFrame ctx ii settings streamNumber st hdr0
    atomically $ writeTQueue outputQ hdrframe
    withFile file ReadMode go
    return ResponseReceived
  where
    -- fixme: more efficient buffering
    einfoEnd = encodeInfo setEndStream streamNumber
    einfo = encodeInfo id streamNumber
    go hdl = do
        bs <- BS.hGet hdl 2048 -- fixme
        loop hdl bs
    loop hdl bs0 = do
        bs <- BS.hGet hdl 2048 -- fixme
        if BS.null bs then do
            let datframe = encodeFrame einfoEnd $ DataFrame bs0
            atomically $ writeTQueue outputQ datframe
          else do
            let datframe = encodeFrame einfo $ DataFrame bs0
            atomically $ writeTQueue outputQ datframe
            writeIORef streamActivity Active
            loop hdl bs

enqueueRsp ctx@Context{..} ii settings Stream{..} (ResponseStream st hdr0 sb) = do
    hdrframe <- headerFrame ctx ii settings streamNumber st hdr0
    atomically $ writeTQueue outputQ hdrframe
    writeIORef streamActivity Active
    sb send $ return ()
    flush'
    return ResponseReceived
  where
    send bb = do
        atomically $ writeTQueue outputQ datframe
        writeIORef streamActivity Active
      where
        einfo = encodeInfo id streamNumber
        datframe = encodeFrame einfo $ DataFrame $ toByteString bb
    -- fixme: 0-length body is inefficient
    flush' = atomically $ writeTQueue outputQ datframe
      where
        einfo = encodeInfo (setEndStream . setPadded) streamNumber
        datframe = encodeFrame einfo $ DataFrame "\5DUMMY"

-- HTTP/2 does not support ResponseStream and ResponseRaw.
enqueueRsp _ _ _ _ _ = -- fixme
    return ResponseReceived

-- fixme: continue
headerFrame :: Context -> InternalInfo -> S.Settings -> Int -> H.Status -> H.ResponseHeaders -> IO ByteString
headerFrame Context{..} ii settings stid st hdr0 = do
    hdr1 <- addServerAndDate hdr0
    let hdr2 = (":status", status) : map (first foldedCase) hdr1
    ehdrtbl <- readIORef encodeDynamicTable
    (ehdrtbl',hdrfrg) <- encodeHeader defaultEncodeStrategy ehdrtbl hdr2
    writeIORef encodeDynamicTable ehdrtbl'
    return $ encodeFrame einfo $ HeadersFrame Nothing hdrfrg
  where
    dc = dateCacher ii
    rspidxhdr = indexResponseHeader hdr0
    defServer = S.settingsServerName settings
    addServerAndDate = addDate dc rspidxhdr . addServer defServer rspidxhdr
    status = B8.pack $ show $ H.statusCode st
    einfo = encodeInfo setEndHeader stid
