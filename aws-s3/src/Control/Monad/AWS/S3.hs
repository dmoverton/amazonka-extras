{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}

module Control.Monad.AWS.S3
  ( MonadS3(..)
  , BucketName(..)
  , ObjectKey(..)
  , ObjectVersion(..)
  , ObjectVersionId(..)
  , handle404
  ) where

import           Control.Error.Util (hush)
import           Control.Lens (each, filtered, toListOf, (&), (.~), (<&>), (?~), (^.))
import           Control.Monad (unless, void)
import           Control.Monad.Catch (MonadCatch(..), MonadThrow(..), tryJust)
import           Control.Monad.Trans (MonadIO(..), MonadTrans(..))
import           Control.Monad.Trans.AWS (AWSConstraint, AWST', Error(..), HasEnv, ToHashedBody)
import qualified Control.Monad.Trans.AWS as Aws
import           Control.Monad.Trans.Resource (MonadResource, liftResourceT, register)
import           Data.ByteString (ByteString)
import           Data.Conduit (ConduitT, transPipe)
import qualified Data.Conduit.Binary as CB
import           Data.List.NonEmpty (NonEmpty, nonEmpty)
import           Data.Maybe (mapMaybe)
import           Network.AWS.Data.Body (RqBody(..), RsBody(..), toHashed)
import           Network.AWS.Data.Text (toText)
import           Network.AWS.S3 (BucketName(..), ObjectKey(..), ObjectVersionId(..))
import qualified Network.AWS.S3 as S3
import           Network.HTTP.Types.Status (notFound404)
import qualified System.Directory as Directory
import           System.IO.Error (isDoesNotExistError)

{- NOTE: listObjectVersions

S3's list object versions call behaves slightly differently to the interface
exposed here, it accepts a object key prefix to select subsets of objects in a
bucket, rather than versions of a specific object. This is the reason
listObjectVersions filters the results returned, to avoid results for files
which have the name object key with other text appended. At appears that amazon
return results sorted first by object ket (lexicographically), and then by date,
so (most recent first). This means that results for the exact objectkey are
returned first:

Uploading files in this order:
  foo.txt
  foo.txt
  foo.txt.backup
  foo.txt
  foo.txt.backup

results in these versions:
  foo.txt - v3
  foo.txt - v2
  foo.txt - v1
  foo.txt.backup - v2
  foo.txt.backup - v1

This behaviour isn't documented, so may break if there are other objects which
match the given ObjectKey as their key prefix, or if Amazon ever change things.

TODO: Write tests against AWS S3 to detect if this behaviour ever changes.
-}

class Monad m => MonadS3 m where
  s3GetObject          :: BucketName -> ObjectKey -> Maybe ObjectVersionId -> ConduitT () ByteString m ()
  s3ListObjectVersions :: BucketName -> ObjectKey -> Int -> m (Maybe (NonEmpty ObjectVersion))
  s3SaveObject         :: BucketName -> ObjectKey -> FilePath -> Maybe ObjectVersionId -> m ()
  s3PutObject          :: ToHashedBody a => BucketName -> ObjectKey -> a -> m ()

data ObjectVersion =
  ObjectVersion
    { _ovIsLatest  :: Bool
    , _ovVersionId :: ObjectVersionId }
  deriving (Show, Eq)

instance {-# OVERLAPPABLE #-} (MonadTrans t, Monad (t m), MonadS3 m) => MonadS3 (t m) where
  s3GetObject bucketName objectKey mObjectVersionId = transPipe lift $ s3GetObject bucketName objectKey mObjectVersionId
  s3ListObjectVersions bucketName objectKey maxVersions = lift $ s3ListObjectVersions bucketName objectKey maxVersions
  s3SaveObject bucketName objectKey fileName = lift . s3SaveObject bucketName objectKey fileName
  s3PutObject bucketName objectKey = lift . s3PutObject  bucketName objectKey

instance (MonadCatch m, MonadResource m, HasEnv r) => MonadS3 (AWST' r m) where
  s3GetObject          = getObject
  s3ListObjectVersions = listObjectVersions
  s3SaveObject         = saveObject
  s3PutObject          = putObject

handle404 :: MonadCatch m => m a -> m (Maybe a)
handle404 = fmap hush . tryJust only404
  where
    only404 :: Error -> Maybe ()
    only404 = \case
      ServiceError err | err ^. Aws.serviceStatus == notFound404 -> Just ()
      _ -> Nothing

getObject ::
  ( AWSConstraint r m
  , MonadIO m)
  => BucketName
  -> ObjectKey
  -> Maybe ObjectVersionId
  -> ConduitT () ByteString m ()
getObject bucketName objectKey mObjectVersionId = do
  let request = S3.getObject bucketName objectKey
                & S3.goVersionId .~ mObjectVersionId
  response <- lift $ Aws.send request
  transPipe liftResourceT . _streamBody $ response ^. S3.gorsBody

listObjectVersions ::
  AWSConstraint r m => BucketName -> ObjectKey -> Int -> m (Maybe (NonEmpty ObjectVersion))
listObjectVersions bucketName objectKey maxVersions = do
  let request = S3.listObjectVersions bucketName
                & S3.lPrefix ?~ toText objectKey
                & S3.lMaxKeys ?~ maxVersions
  Aws.send request
    <&> toListOf (S3.lrsVersions . each . filtered (\ov -> ov ^. S3.ovKey == Just objectKey))
    <&> mapMaybe toObjectVersion
    <&> nonEmpty
  where
    toObjectVersion :: S3.ObjectVersion -> Maybe ObjectVersion
    toObjectVersion ov =
      ObjectVersion
        <$> ov ^. S3.ovIsLatest
        <*> ov ^. S3.ovVersionId

saveObject :: AWSConstraint r m => BucketName -> ObjectKey -> FilePath -> Maybe ObjectVersionId -> m ()
saveObject bucketName objectKey filePath mObjectVersionId = do
  void . register $ deleteFileIfExists filePath --Register with MonadResource to cleanup this file later
  let request = S3.getObject bucketName objectKey & S3.goVersionId .~ mObjectVersionId
  response <- Aws.send request
  Aws.sinkBody (response ^. S3.gorsBody) $ CB.sinkFile filePath
  where
    deleteFileIfExists :: FilePath -> IO ()
    deleteFileIfExists filepath =
      Directory.removeFile filepath `catch` handleDoesNotExist

    handleDoesNotExist :: MonadThrow m => IOError -> m ()
    handleDoesNotExist e = unless (isDoesNotExistError e) $ throwM e

putObject ::
  ( AWSConstraint r m
  , ToHashedBody a)
  => BucketName
  -> ObjectKey
  -> a
  -> m ()
putObject bucketName objectKey =
  void . Aws.send . S3.putObject bucketName objectKey . Hashed . toHashed
