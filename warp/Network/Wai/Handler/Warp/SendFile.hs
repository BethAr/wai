{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module Network.Wai.Handler.Warp.SendFile where

import Control.Applicative ((<$>))
import Data.ByteString.Internal (ByteString(..))
import Data.Word (Word8)
import Foreign.C.Types
import Foreign.ForeignPtr (newForeignPtr_)
import Foreign.Ptr (Ptr, castPtr)
import Network.Socket (Socket)
import qualified Network.Wai.Handler.Warp.FdCache as F
import Network.Wai.Handler.Warp.Types
#if WINDOWS
import Network.Sendfile -- fixme
#else
import System.Posix.Types
#endif

defaultSendFile :: Socket -> SendFile
#if WINDOWS
defaultSendFile s path off len act hdr = sendfileWithHeader s path (PartOfFile off len) act hdr
#else
defaultSendFile = undefined -- to be overridden
#endif

#if WINDOWS
setSendFile :: Connection -> Maybe F.MutableFdCache -> Connection
setSendFile conn _ = conn
#else
type SendV = [ByteString] -> IO ()

setSendFile :: Connection -> Maybe F.MutableFdCache -> Connection
setSendFile conn Nothing     = conn
setSendFile conn (Just fdcs) = conn { connSendFile = sendFile fdcs buf siz sendv}
 where
   sendv = connSendMany conn
   siz = connBufferSize conn
   buf = connFileBuffer conn

sendFile :: F.MutableFdCache -> Ptr Word8 -> Int -> SendV
         -> SendFile
sendFile fdcs buf siz sendv path off len act hdr = do
    (fd, fresher) <- F.getFd fdcs path
    sendfileFdWithHeader buf siz sendv fd off len (act>>fresher) hdr

sendfileFdWithHeader :: Ptr Word8 -> Int -> SendV
                     -> Fd -> Integer -> Integer -> IO () -> [ByteString]
                     -> IO ()
sendfileFdWithHeader buf siz sendv fd off0 len0 hook hdr = do
    fptr <- newForeignPtr_ buf
    n <- positionRead fd buf (mini siz len0) off0
    let bs = PS fptr 0 n
        n' = fromIntegral n
    sendv $ hdr ++ [bs]
    loop fptr (len0 - n') (off0 + n')
  where
    mini i n
      | fromIntegral i < n = i
      | otherwise          = fromIntegral n
    loop fptr len off
      | len <= 0  = return ()
      | otherwise = do
          n <- positionRead fd buf (mini siz len) off
          let bs = PS fptr 0 n
              n' = fromIntegral n
          sendv [bs]
          hook
          loop fptr (len - n') (off + n')

positionRead :: Fd -> Ptr Word8 -> Int -> Integer -> IO Int
positionRead (Fd fd) buf siz off =
    fromIntegral <$> c_pread fd (castPtr buf) (fromIntegral siz) (fromIntegral off)

foreign import ccall unsafe "pread"
  c_pread :: CInt -> Ptr CChar -> ByteCount -> FileOffset -> IO ByteCount -- fixme
#endif
