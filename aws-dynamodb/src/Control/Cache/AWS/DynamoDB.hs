{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Cache.AWS.DynamoDB
  ( CacheConfig(..)
  , withCache
  , CacheItem(..)
  , CacheKey(..)
  , InvalidStoredCacheItem(..)
  ) where

import           Control.Lens (Getting, view, (%~), (<&>))
import           Control.Monad.AWS.DynamoDB (MonadDynamo(..), dynamoLookupResult)
import           Control.Monad.Catch (Exception, MonadThrow(..))
-- import           Control.Monad.Thyme (MonadTime(..))
import           Control.Monad.Reader.Class (MonadReader)
import           Data.Text1 (Text1, ToText1(..))
import           Data.Thyme (Days, UTCTime, addDays, posixToUtc, utcTime, utcToPosix, _utctDay)
import           Data.Typeable (Typeable)
import           GHC.Generics (Generic)
import           Network.AWS.DynamoDB.AttributeValue
                  (AttributeValues(..), DynamoEntity(..), FromAttributeValue(..),
                  ToAttributeValue(..), fromDynamoList, (..:), (..=))
import           Test.QuickCheck.Arbitrary.Generic (Arbitrary, GenericArbitrary(..))

newtype CacheConfig k v = CacheConfig
  { _ccTtl :: Days }
  deriving (Show, Eq, Generic)
  deriving (Arbitrary) via GenericArbitrary (CacheConfig k v)

data CacheItem v = CacheItem
  { _ciKey   :: CacheKey
  , _ciValue :: v
  , _ciTtl   :: UTCTime }
  deriving (Show)

newtype CacheKey = CacheKey
  { _cacheKey :: Text1 }
  deriving (Eq, Show)
  deriving newtype (ToAttributeValue, FromAttributeValue)

instance AttributeValues CacheKey where
  toAttributeValues key = fromDynamoList [ "Key" ..= key ]
  fromAttributeValues e = e ..: "Key"

instance (ToAttributeValue v, FromAttributeValue v) => AttributeValues (CacheItem v) where
  toAttributeValues CacheItem{..} =
    fromDynamoList
      [ "Key"   ..= _ciKey
      , "Value" ..= _ciValue
      , "Ttl"   ..= utcToPosix _ciTtl ]
  fromAttributeValues e =
    CacheItem
      <$> e ..: "Key"
      <*> e ..: "Value"
      <*> do posixToUtc <$> e ..: "Ttl"

instance (ToAttributeValue v, FromAttributeValue v) => DynamoEntity (CacheItem v) where
  type Key (CacheItem v) = CacheKey
  getKey = _ciKey

lookupCache ::
  ( MonadDynamo (CacheItem v) m
  , DynamoEntity (CacheItem v)
  , ToText1 k
  , MonadThrow m )
  => CacheConfig k v
  -> k
  -> m (Maybe v)
lookupCache CacheConfig{..} key =
  ddbGet (CacheKey textKey) >>=
    dynamoLookupResult
      (pure Nothing)
      (throwM . InvalidStoredCacheItem textKey)
      (pure . Just . _ciValue)
  where textKey = toText1 key

data InvalidStoredCacheItem = InvalidStoredCacheItem
  { _isciKey         :: Text1
  , _isciErrorString :: String }
  deriving (Eq, Show, Typeable)
  deriving anyclass (Exception)

insertIntoCache ::
  ( MonadDynamo (CacheItem v) m
  , DynamoEntity (CacheItem v)
  , ToText1 k )
  -- , MonadTime m )
  => CacheConfig k v
  -> k
  -> v
  -> m ()
insertIntoCache CacheConfig{..} key value = do
  ttl <- currentTime <&> utcTime . _utctDay %~ addDays _ccTtl
  ddbPut $ CacheItem textKey value ttl
  where textKey = CacheKey $ toText1 key
        currentTime = undefined -- FIXME

-- Turns a function k -> m v into a cached version of itself. The cached version will lookup
-- the key in the given cache before running the given function if it was not found.
-- The result returned from the given function is then stored in the cache for future calls.
withCache ::
  ( MonadReader r m
  , MonadDynamo (CacheItem v) m
  , DynamoEntity (CacheItem v)
  , ToText1 k
  -- , MonadTime m
  , MonadThrow m )
  => Getting (CacheConfig k v) r (CacheConfig k v)
  -> (k -> m v)
  -> k
  -> m v
withCache lens fetch key = do
  cacheConfig@CacheConfig{..} <- view lens
  if _ccTtl <= 0
    then fetch key
    else lookupCache cacheConfig key >>= \case
      Just value -> pure value
      Nothing -> do
        value <- fetch key
        insertIntoCache cacheConfig key value
        pure value
