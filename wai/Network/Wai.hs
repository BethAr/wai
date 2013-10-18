{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE CPP #-}
{-|

This module defines a generic web application interface. It is a common
protocol between web servers and web applications.

The overriding design principles here are performance and generality . To
address performance, this library is built on top of the conduit and
blaze-builder packages.  The advantages of conduits over lazy IO have been
debated elsewhere and so will not be addressed here.  However, helper functions
like 'responseLBS' allow you to continue using lazy IO if you so desire.

Generality is achieved by removing many variables commonly found in similar
projects that are not universal to all servers. The goal is that the 'Request'
object contains only data which is meaningful in all circumstances.

Please remember when using this package that, while your application may
compile without a hitch against many different servers, there are other
considerations to be taken when moving to a new backend. For example, if you
transfer from a CGI application to a FastCGI one, you might suddenly find you
have a memory leak. Conversely, a FastCGI application would be well served to
preload all templates from disk when first starting; this would kill the
performance of a CGI application.

This package purposely provides very little functionality. You can find various
middlewares, backends and utilities on Hackage. Some of the most commonly used
include:

[warp] <http://hackage.haskell.org/package/warp>

[wai-extra] <http://hackage.haskell.org/package/wai-extra>

[wai-test] <http://hackage.haskell.org/package/wai-test>

-}
module Network.Wai
    ( -- * WAI interface
      -- ** Request
      Request
    , requestMethod
    , httpVersion
    , rawPathInfo
    , rawQueryString
    , requestHeaders
    , isSecure
    , remoteHost
    , pathInfo
    , queryString
    , requestBody
    , vault
    , requestBodyLength
      -- ** Response
    , Response
    , responseFile
    , responseBuilder
    , responseLBS
      -- ** Other types
    , Application
    , Middleware
    , FilePart (..)
    , RequestBodyLength (..)
    , WithSource
      -- ** Helper functions
    , responseStatus
      -- ** Defaults
    , defaultRequest
    ) where

import           Blaze.ByteString.Builder     (Builder, fromLazyByteString)
import qualified Data.ByteString              as B
import qualified Data.ByteString.Lazy         as L
import           Data.ByteString.Lazy.Char8   ()
import           Data.Monoid                  (mempty)
import qualified Network.HTTP.Types           as H
import           Network.Socket               (SockAddr (SockAddrInet))
import           Network.Wai.Internal

responseFile :: H.Status -> H.ResponseHeaders -> FilePath -> Maybe FilePart -> Response
responseFile = ResponseFile

responseBuilder :: H.Status -> H.ResponseHeaders -> Builder -> Response
responseBuilder = ResponseBuilder

responseStatus :: Response -> H.Status
responseStatus rsp =
    case rsp of
      ResponseFile    s _ _ _ -> s
      ResponseBuilder s _ _   -> s
      ResponseSource  s _ _   -> s

responseLBS :: H.Status -> H.ResponseHeaders -> L.ByteString -> Response
responseLBS s h = ResponseBuilder s h . fromLazyByteString

type Application = Request -> IO Response

-- | Middleware is a component that sits between the server and application. It
-- can do such tasks as GZIP encoding or response caching. What follows is the
-- general definition of middleware, though a middleware author should feel
-- free to modify this.
--
-- As an example of an alternate type for middleware, suppose you write a
-- function to load up session information. The session information is simply a
-- string map \[(String, String)\]. A logical type signatures for this middleware
-- might be:
--
-- @ loadSession :: ([(String, String)] -> Application) -> Application @
--
-- Here, instead of taking a standard 'Application' as its first argument, the
-- middleware takes a function which consumes the session information as well.
type Middleware = Application -> Application

-- | A default, blank request.
--
-- Since 2.0.0
defaultRequest :: Request
defaultRequest = Request
    { requestMethod = H.methodGet
    , httpVersion = H.http10
    , rawPathInfo = B.empty
    , rawQueryString = B.empty
    , requestHeaders = []
    , isSecure = False
    , remoteHost = SockAddrInet 0 0
    , pathInfo = []
    , queryString = []
    , requestBody = return B.empty
    , vault = mempty
    , requestBodyLength = KnownLength 0
    }
