{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Monad.AWS.S3.Mock
    ( MockS3T(..)
    , _GetObject
    , _ListObjectVersions
    , _SaveObject
    , _PutObject
    , toVersionId
    , runMockS3T
    , execMockS3T
    , evalMockS3T
    , MockGetObject
    , mockGetObjectAnyOk
    , MockListObjectVersions
    , mockListObjectVersionsOk
    , MockSaveObject
    , mockSaveObjectAnyOk
    , MockPutObject
    , mockPutObjectAnyOk
    , MockS3Call(..)
    ) where

import           Control.Lens (makeLenses, makePrisms, use, (<&>), (<<+=), (<>~))
import           Control.Monad.Base (MonadBase)
import           Control.Monad.Catch (MonadCatch, MonadThrow(..))
import           Control.Monad.Except (MonadError)
import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.Reader (MonadReader(..))
import           Control.Monad.Trans (MonadIO, MonadTrans(..))
import           Control.Monad.Trans.State (StateT(..), modify, runStateT)
import           Data.Bifunctor (second)
import           Data.ByteString (ByteString)
import           Data.Conduit (ConduitT, transPipe)
import           Data.Conduit.List (sourceList)
import           Data.List.NonEmpty (NonEmpty, nonEmpty)
import           Network.AWS.Data (toText)
import           Network.AWS.Data.Body (ToHashedBody(..))
import           Network.AWS.S3 (BucketName, ObjectKey, ObjectVersionId(..))

import           Control.Monad.AWS.S3 (MonadS3(..), ObjectVersion)

data MockContext m =
  MockContext
    { _mcMockGetObject          :: MockGetObject m
    , _mcMockListObjectVersions :: MockListObjectVersions m
    , _mcMockSaveObject         :: MockSaveObject m
    , _mcMockPutObject          :: MockPutObject m
    , _mcActualCalls            :: [MockS3Call]
    , _mcObjectVersionCounter   :: Int }

type MockGetObject m          = BucketName -> ObjectKey -> Maybe ObjectVersionId -> ConduitT () ByteString m ()
type MockListObjectVersions m = BucketName -> ObjectKey -> Int -> m (Maybe (NonEmpty ObjectVersion))
type MockSaveObject m         = BucketName -> ObjectKey -> FilePath -> Maybe ObjectVersionId -> m ()
type MockPutObject m          = BucketName -> ObjectKey -> String -> m ()

data MockS3Call =
    GetObject          BucketName ObjectKey (Maybe ObjectVersionId)
  | ListObjectVersions BucketName ObjectKey Int
  | SaveObject         BucketName ObjectKey FilePath (Maybe ObjectVersionId)
  | PutObject          BucketName ObjectKey String ObjectVersionId
  deriving (Show, Eq)

makeLenses ''MockContext
makePrisms ''MockS3Call

newtype MockS3T m a =
  MockS3T (StateT (MockContext m) m a)
  deriving ( Applicative
           , Functor
           , Monad
           , MonadBase b'
           , MonadCatch
           , MonadError e
           , MonadIO
           , MonadReader r
           , MonadThrow )

instance MonadTrans MockS3T where
  lift = MockS3T . lift

instance MonadThrow m => MonadS3 (MockS3T m) where
  s3GetObject bucketName objectKey mObjectVersionId = do
    lift . MockS3T $ modify $ mcActualCalls <>~ [GetObject bucketName objectKey mObjectVersionId]
    mockGetObject <- lift . MockS3T $ use mcMockGetObject
    transPipe lift $ mockGetObject bucketName objectKey mObjectVersionId
  s3ListObjectVersions bucketName objectKey maxVersions =  MockS3T $ do
    modify $ mcActualCalls <>~ [ListObjectVersions bucketName objectKey maxVersions]
    mockListObjectVersions <- use mcMockListObjectVersions
    lift $ mockListObjectVersions bucketName objectKey maxVersions
  s3SaveObject bucketName objectKey filePath mObjectVersionId = MockS3T $ do
    modify $ mcActualCalls <>~ [SaveObject bucketName objectKey filePath mObjectVersionId]
    mockSaveObject <- use mcMockSaveObject
    lift $ mockSaveObject bucketName objectKey filePath mObjectVersionId
  s3PutObject bucketName objectKey raw = MockS3T $ do
    versionId <- mcObjectVersionCounter <<+= 1 <&> toVersionId
    let hashed = show $ toHashed raw
    modify $ mcActualCalls <>~ [PutObject bucketName objectKey hashed versionId]
    mockPutObject <- use mcMockPutObject
    lift $ mockPutObject bucketName objectKey hashed

toVersionId :: Int -> ObjectVersionId
toVersionId = ObjectVersionId . toText

instance PrimMonad m => PrimMonad (MockS3T m) where
  type PrimState (MockS3T m) = PrimState m
  primitive f = MockS3T $ primitive f

runMockS3T ::
  MonadThrow m
  => MockGetObject m
  -> MockListObjectVersions m
  -> MockSaveObject m
  -> MockPutObject m
  -> MockS3T m a
  -> m (a, [MockS3Call])
runMockS3T mockGetObject mockListObjectVersions mockSaveObject mockPutObject (MockS3T s) =
  second _mcActualCalls <$> runStateT s context
  where
    context = MockContext mockGetObject mockListObjectVersions mockSaveObject mockPutObject mempty 0

execMockS3T ::
  MonadThrow m
  => MockGetObject m
  -> MockListObjectVersions m
  -> MockSaveObject m
  -> MockPutObject m
  -> MockS3T m a
  -> m [MockS3Call]
execMockS3T mockGetObject mockListObjectVersions mockSaveObject mockPutObject =
  fmap snd . runMockS3T mockGetObject mockListObjectVersions mockSaveObject mockPutObject

evalMockS3T ::
  MonadThrow m
  => MockGetObject m
  -> MockListObjectVersions m
  -> MockSaveObject m
  -> MockPutObject m
  -> MockS3T m a
  -> m a
evalMockS3T mockGetObject mockListObjectVersions mockSaveObject mockPutObject =
  fmap fst . runMockS3T mockGetObject mockListObjectVersions mockSaveObject mockPutObject

mockGetObjectAnyOk :: Monad m => [ByteString] -> MockGetObject m
mockGetObjectAnyOk chunks _ _ _ = sourceList chunks

mockListObjectVersionsOk :: Monad m => [ObjectVersion] -> MockListObjectVersions m
mockListObjectVersionsOk versions _ _ _ = pure (nonEmpty versions)

mockSaveObjectAnyOk :: Monad m => MockSaveObject m
mockSaveObjectAnyOk _ _ _ _ = pure ()

mockPutObjectAnyOk :: Monad m => MockPutObject m
mockPutObjectAnyOk _ _ _ = pure ()
