{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Control.Monad.AWS.DynamoDB
  ( Existing(..)
  , MonadDynamo(..)
  , DynamoEntity(..)
  , AttributeValues(..)
  , ToAttributeValue(..)
  , FromAttributeValue(..)
  , HasDynamoTableName(..)
  , DynamoAddResult(..)
  , DynamoLookupResult(..)
  , dynamoLookupResult
  , DynamoUpdateIfUnchangedResult(..)
  , TableName
  , applyTableName
  , applyWithOnSave
  , applyWithOnSave'
  ) where

import           Control.Monad.Catch (MonadCatch)
import           Control.Monad.Reader (MonadReader(..))
import           Control.Monad.Trans (MonadTrans(..))
import           Control.Monad.Trans.AWS (AWST', HasEnv)
import           Control.Monad.Trans.Resource (MonadResource(..))
import           Control.Retry (RetryPolicyM, exponentialBackoff)
import           Data.Conduit (ConduitT, transPipe)
import           Data.Functor.Identity (Identity(..))
import           Data.List.NonEmpty (NonEmpty)

import           Network.AWS.DynamoDB.AttributeValue (AttributeValues(..), ToAttributeValue(..))
import           Network.AWS.DynamoDB.Extra
                  (DynamoAddResult(..), DynamoEntity(..), DynamoLookupResult(..),
                  DynamoUpdateIfUnchangedResult(..), Existing(..), FromAttributeValue(..),
                  TableName, defaultAdd, defaultDelete, defaultDeleteBatch, defaultGet,
                  defaultGetBatch, defaultPut, defaultPutBatch, defaultScan, defaultUpdate,
                  defaultUpdateIfUnchanged, dynamoLookupResult, retryUnproccessedGets,
                  retryUnproccessedWrites)

class (Monad m, DynamoEntity a) => MonadDynamo a m where
  ddbGet               :: Key a -> m (DynamoLookupResult a)
  ddbGetBatch          :: NonEmpty (Key a) -> m [Either String a]
  ddbAdd               :: a -> m DynamoAddResult
  ddbUpdate            :: a -> m ()
  ddbUpdateIfUnchanged :: Existing a -> a -> m DynamoUpdateIfUnchangedResult
  ddbPut               :: a -> m ()
  ddbPutBatch          :: NonEmpty a -> m ()
  ddbDelete            :: Key a -> m (DynamoLookupResult a) -- returns the old value, if it existed, before deletion
  ddbDeleteBatch       :: NonEmpty (Key a) -> m ()
  ddbScan              :: ConduitT () a m ()

instance {-# OVERLAPPABLE #-}
  (MonadTrans t, Monad (t m), MonadDynamo a m)
  => MonadDynamo a (t m) where
  ddbGet                 = lift . ddbGet
  ddbGetBatch            = lift . ddbGetBatch
  ddbAdd                 = lift . ddbAdd
  ddbUpdate              = lift . ddbUpdate
  ddbUpdateIfUnchanged o = lift . ddbUpdateIfUnchanged o
  ddbPut                 = lift . ddbPut
  ddbPutBatch            = lift . ddbPutBatch
  ddbDelete              = lift . ddbDelete
  ddbDeleteBatch         = lift . ddbDeleteBatch @a
  ddbScan                = transPipe lift ddbScan

instance
  (MonadCatch m, MonadResource m, HasEnv r, HasDynamoTableName a r, DynamoEntity a, Show (Key a), Show a)
  => MonadDynamo a (AWST' r m) where
  ddbGet k               = applyTableName @a (`defaultGet` k)
  ddbGetBatch ks         = applyTableName @a $ \tableName ->
    retryUnproccessedGets retryPolicy (defaultGetBatch tableName) ks
  ddbDelete k            = applyTableName @a (`defaultDelete` k)
  ddbDeleteBatch ks      = applyTableName @a $ \tableName ->
    retryUnproccessedWrites retryPolicy (defaultDeleteBatch @a tableName) ks
  ddbScan                = applyTableName @a (`defaultScan` Nothing)
  ddbAdd                 = applyWithOnSave @a defaultAdd
  ddbUpdate              = applyWithOnSave @a (`defaultUpdate` mempty)
  ddbUpdateIfUnchanged o = applyWithOnSave @a (`defaultUpdateIfUnchanged` o)
  ddbPut                 = applyWithOnSave @a defaultPut
  ddbPutBatch            = applyWithOnSave' @a $
    retryUnproccessedWrites retryPolicy . defaultPutBatch

retryPolicy :: Monad m => RetryPolicyM m
retryPolicy = exponentialBackoff 100000

-- 'dynamoTableName' and 'applyTableName' have an ambiguous types. In both functions, the type
-- variable 'a', which is the Dynamo entity type, cannot be inferred from usage or specified with
-- explicit type signatures.
--
-- However, the TypeApplications extension in GHC 8 gives callers a way to specify the ambiguous
-- types at the call site. The AllowAmbiguousTypes extension allows us to define these functions
-- here and the TypeApplications extension allows us to call them.
--
-- For example:
--
--     dynamoTableName @WardenTrackingAction wardenConfig
--
--     applyTableName @CurationTrackingJob $ \ curationTableName -> do ...
--
-- Authors of 'HasDynamoTableName' instances don't need to enable these extensions.

class DynamoEntity a => HasDynamoTableName a r where
  dynamoTableName  :: r -> TableName
  dynamoProperties :: r -> Properties a

applyTableName :: forall a b r m.
  (MonadReader r m, HasDynamoTableName a r)
  => (TableName -> m b)
  -> m b
applyTableName f = f =<< reader (dynamoTableName @a)

applyWithOnSave :: forall a b m r.
  (MonadReader r m, HasDynamoTableName a r, DynamoEntity a)
  => (TableName -> a -> m b)
  -> a
  -> m b
applyWithOnSave f =
  applyWithOnSave' ((. runIdentity) . f) . Identity

applyWithOnSave' :: forall a b f m r.
  (MonadReader r m, HasDynamoTableName a r, DynamoEntity a, Functor f)
  => (TableName -> f a -> m b)
  -> f a
  -> m b
applyWithOnSave' f dynamoEntities = do
  tableName  <- reader $ dynamoTableName  @a
  properties <- reader $ dynamoProperties @a
  f tableName $ onSave properties <$> dynamoEntities
