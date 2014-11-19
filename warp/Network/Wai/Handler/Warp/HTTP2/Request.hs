{-# LANGUAGE OverloadedStrings #-}

module Network.Wai.Handler.Warp.HTTP2.Request (
    mkRequest
  , newReadBody
  , MkReq
  , ValidHeaders(..)
  , validateHeadrs
  ) where

import Control.Applicative ((<$>))
import Control.Concurrent.STM
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import Data.CaseInsensitive (mk)
import Data.IORef (IORef, readIORef, newIORef, writeIORef)
import Data.Monoid (mempty)
import Data.Word8 (isUpper,_colon)
import Network.HPACK
import Network.HTTP.Types (RequestHeaders,hRange)
import qualified Network.HTTP.Types as H
import Network.Socket (SockAddr)
import Network.Wai
import Network.Wai.Handler.Warp.HTTP2.Types
import Network.Wai.Handler.Warp.ReadInt
import qualified Network.Wai.Handler.Warp.Settings as S (Settings, settingsNoParsePath)
import Network.Wai.Internal (Request(..))

data ValidHeaders = ValidHeaders {
    vhMethod :: ByteString
  , vhPath   :: ByteString
  , vhAuth   :: Maybe ByteString
  , vhCL     :: Maybe Int
  , vhHeader :: RequestHeaders
  }

type MkReq = ValidHeaders -> IO ByteString -> Request

mkRequest :: S.Settings -> SockAddr -> MkReq
mkRequest settings addr (ValidHeaders m p ma _ hdr) body = req
  where
    (unparsedPath,query) = B8.break (=='?') p
    path = H.extractPath unparsedPath
    req = Request {
        requestMethod = m
      , httpVersion = http2ver
      , rawPathInfo = if S.settingsNoParsePath settings then unparsedPath else path
      , pathInfo = H.decodePathSegments path
      , rawQueryString = query
      , queryString = H.parseQuery query
      , requestHeaders = hdr
      , isSecure = True
      , remoteHost = addr
      , requestBody = body
      , vault = mempty
      , requestBodyLength = ChunkedBody -- fixme
      , requestHeaderHost = ma
      , requestHeaderRange = lookup hRange hdr
      }

----------------------------------------------------------------

data Pseudo = Pseudo {
    colonMethod :: !(Maybe ByteString)
  , colonPath   :: !(Maybe ByteString)
  , colonAuth   :: !(Maybe ByteString)
  , contentLen  :: !(Maybe ByteString)
  }

emptyPseudo :: Pseudo
emptyPseudo = Pseudo Nothing Nothing Nothing Nothing

validateHeadrs :: HeaderList -> Maybe ValidHeaders
validateHeadrs hs = case go hs True (emptyPseudo,id) of
    Just (Pseudo (Just m) (Just p) ma mcl, h)
        -> Just $ ValidHeaders m p ma (readInt <$> mcl) h
    _   -> Nothing
  where
    go [] _ (p,b)         = Just (p,b [])
    go h@((k,v):kvs) True (p,b)
      | k == ":method"    = go kvs True (p { colonMethod = Just v },b)
      | k == ":path"      = go kvs True (p { colonPath   = Just v },b)
      | k == ":authority" = go kvs True (p { colonAuth   = Just v },b)
      | k == ":scheme"    = go kvs True (p,b)
      | isPseudo k        = Nothing
      | otherwise         = go h False (p,b)
    go ((k,v):kvs) False (p,b)
      | isPseudo k        = Nothing
      | k == "connection" = Nothing
      | k == "te"         = if v == "trailers" then
                                go kvs False (p, b . ((mk k,v) :))
                              else
                                Nothing
      | k == "content-length"
                          = go kvs False (p { contentLen = Just v },b)
      | otherwise         = case BS.find isUpper k of
                                 Nothing -> go kvs False (p, b . ((mk k,v) :))
                                 Just _  -> Nothing

    isPseudo "" = False
    isPseudo k  = BS.head k == _colon


----------------------------------------------------------------

newReadBody :: TQueue ByteString -> IO (IO ByteString)
newReadBody q = do
    ref <- newIORef False
    return $ readBody q ref

readBody :: TQueue ByteString -> IORef Bool -> IO ByteString
readBody q ref = do
    eof <- readIORef ref
    if eof then
        return ""
      else do
        bs <- atomically $ readTQueue q
        when (bs == "") $ writeIORef ref True
        return bs
