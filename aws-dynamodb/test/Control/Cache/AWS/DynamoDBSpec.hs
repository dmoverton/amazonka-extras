module Control.Cache.AWS.DynamoDBSpec (spec) where

import           Control.Lens ((<+=))
import           Control.Monad.AWS.DynamoDB.Mock
import           Control.Monad.Reader (runReaderT)
import           Control.Monad.State.Strict (MonadState, runStateT)
import           Control.Monad.Thyme.Mock (runMockTimeT)
import           Data.Bifunctor (first)
import qualified Data.HashMap.Strict as HM
import           Data.Text1 (Text1, toText1)
import           Network.AWS.DynamoDB.AttributeValue (toAttributeValues)
import           Test.Hspec (Spec, context, describe, it, shouldBe, shouldThrow)

import           Control.Cache.AWS.DynamoDB

spec :: Spec
spec =

  context "withCache" $ do

    let runMockTimeT = undefined -- FIXME

    it "Should never call cache when TTL is <= 0" $ do
      let cacheConfig = CacheConfig { _ccTtl = 0 }
          result = first show $
            runMockDynamoT [] $ runMockTimeT "2019-12-19T00:00:00Z" $
              flip runStateT 0 $ flip runReaderT cacheConfig $
                withCache id functionToCache "Key"
      result `shouldBe` Right (("1", 1), [])

    it "Should increment value when functionToCache is called more than once" $ do
      let cacheConfig = CacheConfig { _ccTtl = 0 }
          result = first show $
            runMockDynamoT [] $ runMockTimeT "2019-12-19T00:00:00Z" $
              flip runStateT 0 $ flip runReaderT cacheConfig $ do
                _ <- withCache id functionToCache "Key"
                withCache id functionToCache "Key"
      result `shouldBe` Right (("2", 2), [])

    it "Should add fetch result when cache miss" $ do
      let cacheConfig = CacheConfig { _ccTtl = 1 }
          key = CacheKey "Key"
          cacheItem = CacheItem key ("1" :: Text1) "2019-12-20T00:00:00Z"
          dynamoCalls = [ (DdbRqGet $ toAttributeValues key, DdbRsNone)
                        , (DdbRqPut $ toAttributeValues cacheItem, DdbRsSuccess) ]
          result = first show $
            runMockDynamoT dynamoCalls $
              runMockTimeT "2019-12-19T00:00:00Z" $ flip runStateT 0 $ flip runReaderT cacheConfig $
                withCache id functionToCache "Key"
      result `shouldBe` Right (("1", 1), fst <$> dynamoCalls)

    it "Should retrieve result when cache hit without calling fetch" $ do
      let cacheConfig = CacheConfig { _ccTtl = 1 }
          key = CacheKey "Key"
          cacheItem = CacheItem key ("9" :: Text1) "2019-12-20T00:00:00Z"
          dynamoCalls = [ (DdbRqGet $ toAttributeValues key, DdbRsOne $ toAttributeValues cacheItem) ]
          result = first show $
            runMockDynamoT dynamoCalls $
              runMockTimeT "2019-12-19T00:00:00Z" $ flip runStateT 0 $ flip runReaderT cacheConfig $
                withCache id functionToCache "Key"
      result `shouldBe` Right (("9", 0), fst <$> dynamoCalls)

    it "Should throw error when cached item is invalid" $ do
      let cacheConfig = CacheConfig { _ccTtl = 1 }
          key = CacheKey "Key"
          cacheItem = CacheItem key (HM.singleton ("9" :: Text1) (1 :: Int)) "2019-12-20T00:00:00Z"
          dynamoCalls = [ (DdbRqGet $ toAttributeValues key, DdbRsOne $ toAttributeValues cacheItem) ]
          result =
            runMockDynamoT dynamoCalls $
              runMockTimeT "2019-12-19T00:00:00Z" $ flip runStateT 0 $ flip runReaderT cacheConfig $
                withCache id functionToCache "Key"
      result `shouldThrow` anyInvalidStoredCacheItem

    it "Should throw error when cached item is invalid and include the key" $ do
      let cacheConfig = CacheConfig { _ccTtl = 1 }
          key = CacheKey "SomeWeird:Key"
          cacheItem = CacheItem key (HM.singleton ("9" :: Text1) (1 :: Int)) "2019-12-20T00:00:00Z"
          dynamoCalls = [ (DdbRqGet $ toAttributeValues key, DdbRsOne $ toAttributeValues cacheItem) ]
          result =
            runMockDynamoT dynamoCalls $
              runMockTimeT "2019-12-19T00:00:00Z" $ flip runStateT 0 $ flip runReaderT cacheConfig $
                withCache id functionToCache "SomeWeird:Key"
      result `shouldThrow` invalidStoredCacheItemWithKey "SomeWeird:Key"

anyInvalidStoredCacheItem :: InvalidStoredCacheItem -> Bool
anyInvalidStoredCacheItem = const True

invalidStoredCacheItemWithKey :: Text1 -> InvalidStoredCacheItem -> Bool
invalidStoredCacheItemWithKey key InvalidStoredCacheItem{..} = _isciKey == key

-- Increments the state by one, and returns the new state as Text1
functionToCache :: MonadState Int m => Text1 -> m Text1
functionToCache _ = toText1 <$> do id <+= 1
