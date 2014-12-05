{-# LANGUAGE RecordWildCards, NamedFieldPuns #-}

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
import Data.Binary as Bin
import Data.Version
import Data.Word
import qualified Data.ByteString.Base16 as Hex
import Prelude hiding (lookup)
import System.Directory
import System.FilePath
import System.IO
import System.IO.Error

import qualified Data.HashTable.IO                              as HT
import qualified Data.ByteString.Char8                          as SB
import qualified Data.ByteString.Lazy.Char8                     as B

-- | What method of mapping keys -> files on disk are we using? Whenever the
-- behaviour of 'filenameOfKey' changes, the storage revision needs to be
-- updated.
--
revision :: FilePath
revision = "r1"

filenameOfKey :: (Show key, Binary key, Hashable key) => key -> FilePath
-- RRN: Seems like this isn't guaranteed to be a collission-free hash?
-- filenameOfKey = show . hash
filenameOfKey = SB.unpack . Hex.encode . B.toStrict . Bin.encode

-- | Return a path underneath the one given.  The returned path is the
-- unique file corresponding to the given key.
-- 
-- Note: This may introduce additional directories that need to be created.
pathOfKey :: (Show key, Binary key, Hashable key) =>
             FilePath -> key -> FilePath
pathOfKey storeLocation key = storeLocation </> filenameOfKey key 


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
    -- This maximum applies is for this FileCache object only.

    -- TODO: add Entries maxes as well as bytes:
--  , confMaxDiskEntries      :: Maybe Word
    -- Maximum number of entries stored on disk.
    
  , confMaxSizeBytes    :: Maybe Word64
    -- ^ Maxmium number of bytes on disk to use for this project.
    -- This maximum applies only to this "project" (but to all versions of it).
  }
 deriving (Show, Read, Eq, Ord)

instance Default FileCacheConfig where
  def = FileCacheConfig Nothing Nothing Nothing Nothing Nothing

-- | The structure that stores in-memory cached data that is read from disk.
--
data FileCache key val = FileCache
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
  {
    storeLocation       :: FilePath
    -- ^ The path to a directory that contains all key/value pairs as
    -- well as any meta-data files or locks for this `FileCache`.
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
    --     There may be no right choice for all use cases :
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

instance Show (FileCache key val) where
  show (FileCache{storeLocation}) = "<filestore "++storeLocation++">"

-------------------------------------------------------------------------------

-- | Construct a 'FileCache' at the given location on disk, or at a
-- default location in the user's home directory, if unspecified.
--
new :: FileCacheConfig -> IO (FileCache key val)
new storeConfig@FileCacheConfig{..} = do
  storeCache    <- newMVar =<< HT.new
  base          <- case confBasePath of
                     Just p     -> return p
                     Nothing    -> getAppUserDataDirectory ("file-cache")
  let storeLocation = base </> revision </>
                      fromMaybe "noproj" (fmap ("proj_"++) confName) </>
                      maybe "no_version" showVersion confVersion
  createDirectoryIfMissing True storeLocation
  --
  return $ FileCache{..}


-- | Lookup a key in the FileCache.  This may or may not require
-- loading from disk.  
lookup :: (Eq key, Ord key, Hashable key, Binary key, Show key, Binary val) =>
          key -> FileCache key val -> IO (Maybe val)
lookup key fc =
  do res <- lookupFull key fc
     case res of
       (Nothing, Nothing) -> return Nothing
       (Just memhit,_)    -> return (Just memhit)
       (Nothing, Just diskhit) -> do
         -- FIXME: catch all IO errors here!
           dat <- B.readFile diskhit
         -- TODO: Switch to an mmap-based file reading approach.
           let val = either error Just (decode dat)
           withMVar (storeCache fc) $ \sc -> HT.insert sc key val
           return val


-- | A version of `lookup` that returns whether there was a hit in the
-- memory or disk levels of cache.  This version does not read any
-- values from disk to memory.
lookupFull :: (Eq key, Ord key, Hashable key, Binary key, Show key, Binary val) =>
              key -> FileCache key val -> IO (Maybe val, Maybe FilePath)
-- What should we return here? The FilePath (exposing details of where
-- things are stored on disk) or load the file into memory and return
-- a (strict) ByteString?
--
-- In the latter case, do we need a third policy of what is the maximum
-- in-memory size that we cache?
--
lookupFull key FileCache{..} = do
  -- Check the local cache:
  mval <- withMVar storeCache $ \sc -> HT.lookup sc key
  let fp = pathOfKey storeLocation key 
  case mval of
    -- Local cache hit _and_ already loaded into memory
    Just (Just v) -> return (Just v, Just fp)

    -- Check whether the file was stored to disk by a concurrent client. This is
    -- also the fall-through for cache-hit but not loaded, because we need to
    -- read the file and update the local cache in either case.
    _             -> do
      yes <- doesFileExist fp
      if yes
         then return (Nothing, Just fp)
         else return (Nothing, Nothing)



-- | Store a key-value pair onto disk in the given FileCache.  This
-- function is synchronous and returns only after the disk operation
-- is complete.
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
store :: (Eq key, Ord key, Hashable key, Binary key, Show key, Binary val) =>
         key -> val -> FileCache key val -> IO ()
-- TODO: Even if we don't need locking, we may need extra metadata
-- files on disk to record timestamps or sum up disk usage
-- efficiently: for example, by keeping a bytes sum at every directory
-- level and updating all log(N) files when we write a key.
store k v fc@FileCache{..} = do
  tmp <- bracket
           (openBinaryTempFile storeLocation (filenameOfKey k))
           (\(_,  h) -> hClose h)
           (\(fp, h) -> do B.hPut h (encode v)
                           return fp)
-- FIXME: update the hash table!
  storeFile k tmp fc


-- | Copy an already on-disk file to the file store.
--
-- The location to which files are added 
-- Adding new files asserts the versioning policy.
storeFile :: (Eq key, Ord key, Hashable key, Binary key, Show key) =>
             key -> FilePath -> FileCache key val -> IO ()
storeFile k src FileCache{..} = do
  let dst = pathOfKey storeLocation k
  -- TODO: test if it is already a path under storeLocation and avoid this extra copy:
  (tmp,h) <- openBinaryTempFile storeLocation (filenameOfKey k)
  hClose h
  (do copyFile src dst
      renameFile src dst)
    `catchIOError` (\_ -> 
      error "FINISHME: need a retry strategy here on failed IO")
--      copyFile src dst  -- copy to temporary location in destination directory and then move..
--      removeFile src

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
