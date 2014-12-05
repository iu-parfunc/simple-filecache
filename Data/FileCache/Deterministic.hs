{-# LANGUAGE RecordWildCards #-}

{-|

A library for caching artifacts on disk.  This library assumes a
deterministic key->value mapping, so it is an error if any program or
multiple programs ever write different values to the same key.

This module is intented to be imported qualified.

-}

module Data.FileCache.Deterministic
       ( -- * Types
         ProjName, ProjVersion, FileCacheConfig(..), FileCache,

         -- * Operations
         new, lookup, store, storeFile
       )
       where

import Control.Concurrent.MVar
import Control.Exception
import Data.Default
import Data.Hashable
import Data.Maybe
import Data.Binary
import Data.Version
import Data.Word
import Prelude hiding (lookup)
import System.Directory
import System.FilePath
import System.IO
import System.IO.Error

import qualified Data.HashTable.IO                              as HT
-- import qualified Data.ByteString.Char8                       as B
import qualified Data.ByteString.Lazy.Char8                     as B

-- | What method of mapping keys -> files on disk are we using? Whenever the
-- behaviour of 'filenameOfKey' changes, the storage revision needs to be
-- updated.
--
revision :: FilePath
revision = "r1"

-- RRN: Seems like this isn't guaranteed to be unique?
filenameOfKey :: Hashable key => key -> FilePath
filenameOfKey = show . hash


{--
-- A key must be serializable, but there are some good reasons NOT to
-- force it to be pre-serialized.
type Key = B.ByteString
--}



type ProjName    = String
type ProjVersion = Version

type HashTable key val = HT.BasicHashTable key val

data FileCacheConfig = FileCacheConfig {
    confName            :: Maybe ProjName
    -- ^ The name under which our keys should be grouped.  Any size
    -- limits for the on-disk cache are per-project.

  , confVersion         :: Maybe ProjVersion
    -- ^ The value associated with a given key may change when the
    -- `ProjVersion` changes, but not otherwise.
    
      -- TLM: well, this is more like what version of the keys I can handle, not
      --      necessarily related to the version of my project. For example, we
      --      might increment this only when the key-generation mechanism in
      --      accelerate-cuda changes, or code generation changes for the same
      --      key (if we are hashing not the source but say the AST), or when we
      --      detect a different version of 'nvcc'. In this final sense, this is
      --      (perhaps) a property of an individual 'insert', and so maybe that
      --      case just requires the user to include this as part of the key.
      --

  , confBasePath        :: Maybe FilePath
      -- ^ A location on disk for the file store. If this is `Nothing`, then a
      --   default, per-user location is used.

  , confMaxMemBytes :: Maybe Word
    -- ^ Maximum number of bytes of memory to use for caching on-disk values.
    
    -- TODO: gc policy? Should the limits be stored as part of the cache, and
    --       asserted on every insert?

    -- TODO: add Entries maxes as well as bytes:
--  , confMaxDiskEntries      :: Maybe Word
    -- Maximum number of entries stored on disk.
    
  , confMaxSizeBytes    :: Maybe Word64
    -- ^ Maxmium number of bytes on disk to use for this project.
  }

instance Default FileCacheConfig where
  def = FileCacheConfig Nothing Nothing Nothing Nothing Nothing

-- | The structure that stores the in-memory cached data that is read from disk.
--
-- TLM: I'm a bit confused by this now. RE. Persistent.hs from acc-cuda, I
--      thought we were in favour of getting rid of the index file, and then
--      lookup corresponds to "does file exist on disk", which is safe to
--      concurrent access?
--
--      I guess, if we don't have delete we can cache the keys we know about on
--      startup for faster initial lookup.
--
--      As I think about what fields should be in here, it looks very much like
--      the FileCacheConfig object above.
--
data FileCache key val = FileCache
  {
    storeLocation       :: FilePath
  , storeConfig         :: FileCacheConfig
    -- ^ Keep the config we were created with, for future reference.
    
    -- TLM: questions for 'storeCache'
    --
    --  1. Do we want to cache the entries that have been read from disk?
    --
    --  2. If so, how strict will we be in maintaining the store limits if there
    --     are potentially several clients putting things into the cache
    --     concurrently.
    --
    --     a. On insertion/store, do I completely update my local 'storeCache'
    --        to be consistent with what is on disk, and then apply the policy?
    --        This sounds like an expensive operation.
    --
    --     b. On insertion/store, do I not care about the global view and just
    --        update my local cache, and apply the policy based on that? This
    --        means that two clients could have local views each within the
    --        policy limits but the union of which would exceed said limits.
    --
    --     c. Have a separate thread that in the background periodically applies
    --        the versioning/limits policy based on the on-disk state.
    --
    --     There may be no right choice for all use cases ):
    --
    --     On the other hand, what is the performance penalty for not having a
    --     local in-memory cache, and hitting disk every time? This also depends
    --     on the answer to the question of whether or not 'lookup' reads the
    --     file from disk, or just returns the path to said file (but then, can
    --     we guarantee that that path will remain valid?)
    --
    --  3. When we add a file to the store, should we add it to the
    --     'storeCache'? This particularly applies to storing a file that might
    --     not exist in memory at all. Additionally, maybe the user wants to
    --     store files to disk so that they can be _purged_ from memory, and
    --     this certainly would defeat that.
    --
  , storeCache          :: MVar ( HashTable key (Maybe val) )
  }

-------------------------------------------------------------------------------

-- | Construct a 'FileCache' at the given location on disk, or at a
-- default location in the user's home directory, if unspecified.
--
new :: FileCacheConfig -> IO (FileCache key val)
new storeConfig@FileCacheConfig{..} = do
  storeCache    <- newMVar =<< HT.new
  base          <- case confBasePath of
                     Just p     -> return p
                     Nothing    -> getAppUserDataDirectory ("file-cache" </> revision)
  let storeLocation = base </> fromMaybe "" confName </>  maybe "" showVersion confVersion
  createDirectoryIfMissing True storeLocation
  --
  return $ FileCache{..}


-- | Lookup a key in the FileCache.  This may or may not require
-- loading from disk.  
lookup :: (Eq key, Ord key, Hashable key, Binary val) =>
          key -> FileCache key val -> IO (Maybe val)
-- What should we return here? The FilePath (exposing details of where
-- things are stored on disk) or load the file into memory and return
-- a (strict) ByteString?
--
-- In the latter case, do we need a third policy of what is the maximum
-- in-memory size that we cache?
--
lookup key FileCache{..} = do
  -- Check the local cache
  mval <- withMVar storeCache $ \sc -> HT.lookup sc key
  case mval of
    -- Local cache hit _and_ already loaded into memory
    Just (Just v) -> return (Just v)

    -- Check whether the file was stored to disk by a concurrent client. This is
    -- also the fall-through for cache-hit but not loaded, because we need to
    -- read the file and update the local cache in either case.
    _             -> do
      let fp = storeLocation </> filenameOfKey key
      yes <- doesFileExist fp
      if not yes
         then return Nothing
         else do
           dat <- B.readFile fp
           let val = either error Just (decode dat)
           withMVar storeCache $ \sc -> HT.insert sc key val
           return val


-- | Store a key-value pair onto disk in the given FileCache.
--
-- If there is already an entry for the key on disk, then `store`
-- *compares* the old and new values.  If they differ, an error is
-- thrown.  Thus, the on-disk entry behaves like an "IVar".  This
-- policy dynamically enforces the determinism contract.
--
-- UNFINISHED:
--
-- Note, that if memory usage is capped for this project, inserting a
-- new entry may cause older entries to be deleted with a
-- least-recently-used (LRU) replacement policy.
store :: (Eq key, Hashable key, Binary val) =>
         key -> val -> FileCache key val -> IO ()
-- TODO: Even if we don't need locking, we may need extra metadata
-- files on disk to record timestamps or sum up 
store k v fc@FileCache{..} = do
  tmp <- bracket
           (openBinaryTempFile storeLocation (filenameOfKey k))
           (\(_,  h) -> hClose h)
           (\(fp, h) -> do B.hPut h (encode v)
                           return fp)
  storeFile k tmp fc


-- | Copy an already on-disk file to the file store.
-- 
-- Adding new files asserts the versioning policy.
storeFile :: (Eq key, Hashable key) => key -> FilePath -> FileCache key val -> IO ()
storeFile k src FileCache{..} = do
  let dst = storeLocation </> filenameOfKey k
  renameFile src dst
    `catchIOError` \_ -> do
      copyFile src dst  -- copy to temporary location in destination directory and then move?
      removeFile src

  -- TODO: Update the storeCache? See questions above.

  -- TODO: apply versioning policy




-- | This is a version of lookup that does not enforce determinism at
-- runtime.  That is, if it finds an existing entry for a key with a
-- non-matching value, it does not check it for equality.
--
-- Use this only if you are SURE that the only nondeterminism present
-- is benign (e.g. date/time stamps being included in value).
--
-- TLM: I'm not sure what this means?
--
-- unsafeLookup :: (Eq key, Serializable key) => (ProjName,ProjVersion) -> key -> IO (Maybe val)
-- unsafeLookup = undefined

-- unsafeLookup :: (Eq key, Serializable key) => key -> FileCache key val -> IO (Maybe val)

-- (require 'evil)




{-
_ = do
   cabalD <- getAppUserDataDirectory "cabal"
   let tokenD = cabalD </> "googleAuthTokens"
       tokenF = tokenD </> clientId client <.> "token"
   d1       <- doesDirectoryExist cabalD     
   unless d1 $ createDirectory cabalD -- Race.
   d2       <- doesDirectoryExist tokenD 
   unless d2 $ createDirectory tokenD -- Race.
   f1       <- doesFileExist tokenF
   if f1 then do
      str <- readFile tokenF
      case reads str of
        -- Here's our approach to versioning!  If we can't read it, we remove it.
        ((oldtime,toks),_):_ -> do
          tagged <- checkExpiry tokenF (oldtime,toks)
          return (snd tagged)
        [] -> do
          putStrLn$" [getCachedTokens] Could not read tokens from file: "++ tokenF
          putStrLn$" [getCachedTokens] Removing tokens and re-authenticating..."
          removeFile tokenF 
          getCachedTokens client
    else do 
     toks <- askUser
     fmap snd$ timeStampAndWrite tokenF toks
--}
