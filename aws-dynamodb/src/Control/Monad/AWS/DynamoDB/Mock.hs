{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Control.Monad.AWS.DynamoDB.Mock
  ( MockDynamoRequest(..)
  , DdbOtherRequest(..)
  , MockDynamoResponse(..)
  , MockDynamoT
  , DynamoExpectedRequestNotFound(..)
  , runMockDynamoT
  , execMockDynamoT
  , evalMockDynamoT
  , mockDdbGet
  , mockDdbGetBatch
  , mockDdbAdd
  , mockDdbUpdate
  , mockDdbUpdateIfUnchanged
  , mockDdbPut
  , mockDdbDelete
  , mockDdbScan
  , ddbRsFailure
  , matchTestCall
  , patternMatchFail
  ) where

import           Control.Exception (Exception(..), PatternMatchFail(..), SomeException(..))
import           Control.Lens ((<>~))
import           Control.Lens.TH (makeLensesFor)
import           Control.Monad.Base (MonadBase)
import           Control.Monad.Catch (MonadCatch, MonadThrow(..))
import           Control.Monad.Except (MonadError)
import           Control.Monad.Morph (MFunctor)
import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.Reader (MonadReader(..))
import           Control.Monad.Trans (MonadIO, MonadTrans(..))
import           Control.Monad.Trans.State (StateT(..), evalStateT, execStateT, modify, runStateT)
import qualified Control.Monad.Trans.State as State
import           Data.Aeson (encode)
import           Data.Bifunctor (second)
import qualified Data.ByteString.Lazy.Char8 as BS
import           Data.Conduit (ConduitT)
import qualified Data.Conduit.List as CL
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.These (These(..))

import           Control.Monad.AWS.DynamoDB (DynamoAddResult(..), MonadDynamo(..))
import           Network.AWS.DynamoDB.Extra
                  (DynamoEntity(..), DynamoLookupResult(..),
                  DynamoUpdateIfUnchangedResult(..), Existing(..), makeUnparseableUnprocessedItems,
                  makeUnprocessedItems)
import           Network.AWS.DynamoDB.AttributeValue (DynamoObject, AttributeValues(..), parseEither)

data MockContext = MockContext
  { _mcExpectedCalls :: [(MockDynamoRequest, MockDynamoResponse)]
  , _mcActualCalls   :: [MockDynamoRequest] }

data MockDynamoRequest
  = DdbRqGet               DynamoObject
  | DdbRqGetBatch          (NonEmpty DynamoObject)
  | DdbRqAdd               DynamoObject
  | DdbRqUpdate            DynamoObject
  | DdbRqUpdateIfUnchanged DynamoObject DynamoObject
  | DdbRqPut               DynamoObject
  | DdbRqPutBatch          (NonEmpty DynamoObject)
  | DdbRqDelete            DynamoObject
  | DdbRqDeleteBatch       (NonEmpty DynamoObject)
  | DdbRqScan
  | DdbRqOther             DdbOtherRequest
  deriving (Eq)

data DdbOtherRequest = forall a. Show a => DdbOtherRequest a

instance Show DdbOtherRequest where
  showsPrec p (DdbOtherRequest a) = showsPrec p a

-- Crude Eq instance using show, since we cannot directly compare a and b since their types
-- could be different
instance Eq DdbOtherRequest where
  DdbOtherRequest a == DdbOtherRequest b = show a == show b

instance Show MockDynamoRequest where
  show = \case
    DdbRqGet                hm              -> BS.unpack $ "DdbRqGet " <> encode hm
    DdbRqGetBatch           hms             -> BS.unpack $ "DdbRqGetBatch " <> BS.unlines (NE.toList $ encode <$> hms)
    DdbRqAdd                hm              -> BS.unpack $ "DdbRqAdd " <> encode hm
    DdbRqUpdate             hm              -> BS.unpack $ "DdbRqUpdate " <> encode hm
    DdbRqUpdateIfUnchanged  existing update -> BS.unpack $ "DdbRqUpdateIfUnchanged " <> encode existing <> " | " <> encode update
    DdbRqPut                hm              -> BS.unpack $ "DdbRqPut " <> encode hm
    DdbRqPutBatch           hms             -> BS.unpack $ "DdbRqPutBatch " <> BS.unlines (NE.toList $ encode <$> hms)
    DdbRqDelete             hm              -> BS.unpack $ "DdbRqDelete " <> encode hm
    DdbRqDeleteBatch        hms             -> BS.unpack $ "DdbRqDeleteBatch " <> BS.unlines (NE.toList $ encode <$> hms)
    DdbRqScan                               -> "DdbRqScan"
    DdbRqOther              rq              -> "DdbRqOther " ++ show rq

data MockDynamoResponse
  = DdbRsNone
  | DdbRsOne     DynamoObject
  | DdbRsMany    (NonEmpty DynamoObject)
  | DdbRsSuccess
  | DdbRsFailure SomeException
  | DdbRsConditionFailure

instance Show MockDynamoResponse where
  show = \case
    DdbRsNone             -> "DdbRsNone"
    DdbRsOne hm           -> BS.unpack $ "DdbRsOne " <> encode hm
    DdbRsMany hms         -> BS.unpack $ "DdbRsMany " <> encode hms
    DdbRsSuccess          -> "DdbRsSuccess"
    DdbRsFailure e        -> "DdbRsFailure " ++ show e
    DdbRsConditionFailure -> "DdbRsConditionFailure"

makeLensesFor [("_mcActualCalls", "mcActualCalls")] ''MockContext

newtype MockDynamoT m a =
  MockDynamoT (StateT MockContext m a)
  deriving ( Applicative
           , Functor
           , Monad
           , MonadBase b'
           , MonadCatch
           , MonadError e
           , MonadIO
           , MonadReader r
           , MonadThrow
           , MonadTrans
           , MFunctor )

instance (MonadThrow m, DynamoEntity a, Show a, Show (Key a)) => MonadDynamo a (MockDynamoT m) where
  ddbGet               = mockDdbGet
  ddbGetBatch          = mockDdbGetBatch
  ddbAdd               = mockDdbAdd
  ddbUpdate            = mockDdbUpdate
  ddbUpdateIfUnchanged = mockDdbUpdateIfUnchanged
  ddbPut               = mockDdbPut
  ddbPutBatch          = mockDdbPutBatch
  ddbDelete            = mockDdbDelete
  ddbDeleteBatch       = mockDdbDeleteBatch @a
  ddbScan              = mockDdbScan

instance PrimMonad m => PrimMonad (MockDynamoT m) where
  type PrimState (MockDynamoT m) = PrimState m
  primitive f = MockDynamoT $ primitive f

runMockDynamoT ::
  MonadThrow m
  => [(MockDynamoRequest, MockDynamoResponse)]
  -> MockDynamoT m a
  -> m (a, [MockDynamoRequest])
runMockDynamoT expectedCalls (MockDynamoT s) =
  second _mcActualCalls <$> runStateT s (MockContext expectedCalls [])

execMockDynamoT ::
  MonadThrow m
  => [(MockDynamoRequest, MockDynamoResponse)]
  -> MockDynamoT m a
  -> m [MockDynamoRequest]
execMockDynamoT expectedCalls (MockDynamoT s) =
  _mcActualCalls <$> execStateT s (MockContext expectedCalls [])

evalMockDynamoT ::
  MonadThrow m
  => [(MockDynamoRequest, MockDynamoResponse)]
  -> MockDynamoT m a
  -> m a
evalMockDynamoT expectedCalls (MockDynamoT s) = evalStateT s $ MockContext expectedCalls []

ddbRsFailure :: Exception e => e -> MockDynamoResponse
ddbRsFailure e = DdbRsFailure $ SomeException e

mockDdbGet :: (MonadThrow m, DynamoEntity a) => Key a -> MockDynamoT m (DynamoLookupResult a)
mockDdbGet k =
  matchTestCall (DdbRqGet $ toAttributeValues k) >>= \case
    DdbRsNone  -> pure NotFound
    DdbRsOne a -> pure $ either ParseError Found $ parseEither fromAttributeValues a
    _          -> throwM $ patternMatchFail "Get"

mockDdbGetBatch ::
  (MonadThrow m, DynamoEntity a) => NonEmpty (Key a) -> MockDynamoT m [Either String a]
mockDdbGetBatch ks =
  matchTestCall (DdbRqGetBatch $ toAttributeValues <$> ks) >>= \case
    DdbRsNone    -> pure mempty
    DdbRsMany as -> pure $ parseEither fromAttributeValues <$> NE.toList as
    _            -> throwM $ patternMatchFail "GetBatch"

mockDdbAdd :: (MonadThrow m, AttributeValues a) => a -> MockDynamoT m DynamoAddResult
mockDdbAdd a =
  matchTestCall (DdbRqAdd $ toAttributeValues a) >>= \case
    DdbRsSuccess          -> pure DynamoAdded
    DdbRsConditionFailure -> pure DynamoAlreadyExists
    _                     -> throwM $ patternMatchFail "Add"

mockDdbUpdate :: (MonadThrow m, AttributeValues a) => a -> MockDynamoT m ()
mockDdbUpdate a =
  matchTestCall (DdbRqUpdate $ toAttributeValues a) >>= \case
    DdbRsSuccess -> pure ()
    _            -> throwM $ patternMatchFail "Update"

mockDdbUpdateIfUnchanged ::
  (MonadThrow m, AttributeValues a)
  => Existing a
  -> a
  -> MockDynamoT m DynamoUpdateIfUnchangedResult
mockDdbUpdateIfUnchanged (Existing old) new =
  matchTestCall (DdbRqUpdateIfUnchanged (toAttributeValues old) (toAttributeValues new)) >>= \case
    DdbRsSuccess          -> pure DynamoUpdateSuccessful
    DdbRsConditionFailure -> pure DynamoUpdateDirtyRead
    _                     -> throwM $ patternMatchFail "UpdateIfUnchanged"

mockDdbPut :: (MonadThrow m, AttributeValues a) => a -> MockDynamoT m ()
mockDdbPut a =
  matchTestCall (DdbRqPut $ toAttributeValues a) >>= \case
    DdbRsSuccess -> pure ()
    _            -> throwM $ patternMatchFail "Put"

mockDdbPutBatch :: forall a m.
  ( MonadThrow m
  , DynamoEntity a
  , Show a)
  => NonEmpty a
  -> MockDynamoT m ()
mockDdbPutBatch as =
  matchTestCall (DdbRqPutBatch $ toAttributeValues <$> as) >>= \case
    DdbRsSuccess -> pure ()
    DdbRsNone -> pure ()
    DdbRsMany unprocessed ->
      case NE.partitionEithers $ parseEither fromAttributeValues <$> unprocessed of
        That unprocessedItems ->
          throwM $ makeUnprocessedItems as unprocessedItems
        This parseErrors ->
          throwM $ makeUnparseableUnprocessedItems as parseErrors []
        These parseErrors unprocessedItems ->
          throwM $ makeUnparseableUnprocessedItems as parseErrors $ NE.toList unprocessedItems
    _ -> throwM $ patternMatchFail "PutBatch"

mockDdbDelete :: (MonadThrow m, DynamoEntity a) => Key a -> MockDynamoT m (DynamoLookupResult a)
mockDdbDelete k =
  matchTestCall (DdbRqDelete $ toAttributeValues k) >>= \case
    DdbRsNone  -> pure NotFound
    DdbRsOne a -> pure $ either ParseError Found $ parseEither fromAttributeValues a
    _          -> throwM $ patternMatchFail "Delete"

mockDdbDeleteBatch :: forall a m.
  ( MonadThrow m
  , DynamoEntity a
  , Show (Key a))
  => NonEmpty (Key a)
  -> MockDynamoT m ()
mockDdbDeleteBatch ks =
  matchTestCall (DdbRqDeleteBatch $ toAttributeValues <$> ks) >>= \case
    DdbRsSuccess -> pure ()
    DdbRsNone -> pure ()
    DdbRsMany unproccessed ->
      case NE.partitionEithers $ parseEither fromAttributeValues <$> unproccessed of
        That unprocessedKeys ->
          throwM $ makeUnprocessedItems ks unprocessedKeys
        This parseErrors ->
          throwM $ makeUnparseableUnprocessedItems ks parseErrors []
        These parseErrors unprocessedKeys ->
          throwM $ makeUnparseableUnprocessedItems ks parseErrors $ NE.toList unprocessedKeys
    _ -> throwM $ patternMatchFail "DeleteBatch"

mockDdbScan :: (MonadThrow m, AttributeValues a) => ConduitT () a (MockDynamoT m) ()
mockDdbScan =
  lift (matchTestCall DdbRqScan) >>= \case
    DdbRsNone    -> pure ()
    DdbRsMany as -> either fail CL.sourceList $ parseEither fromAttributeValues `mapM` NE.toList as
    _            -> throwM $ patternMatchFail "Scan"

matchTestCall :: MonadThrow m => MockDynamoRequest -> MockDynamoT m MockDynamoResponse
matchTestCall request = do
  MockContext{..} <- MockDynamoT State.get
  MockDynamoT $ modify (mcActualCalls <>~ [request])
  case lookup request _mcExpectedCalls of
    Nothing                               -> throwM $ DynamoExpectedRequestNotFound
                                                        (show request)
                                                        (show _mcExpectedCalls)
    Just (DdbRsFailure (SomeException e)) -> throwM e
    Just response                         -> pure response

data DynamoExpectedRequestNotFound = DynamoExpectedRequestNotFound
  { _dernfRequest :: String
  , _dernfMocks   :: String }
  deriving (Show)
  deriving anyclass (Exception)

patternMatchFail :: String -> PatternMatchFail
patternMatchFail functionName =
  PatternMatchFail $ "Missing or incorrect DynamoDB mock " <> functionName <> " response type"
