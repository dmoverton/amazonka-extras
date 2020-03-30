{-# LANGUAGE OverloadedLists #-}

module Control.Monad.AWS.S3.MockSpec (spec) where

import           Control.Monad.Catch (MonadThrow(..))
import           Data.ByteString (ByteString)
import           Data.Conduit (ConduitT, sourceToList)
import           Data.Conduit.List (sourceList)
import           Network.AWS.Data.Body (toHashed)
import           Network.AWS.S3 (BucketName(..), ObjectKey(..))
import           Network.AWS.Types (Error(..), ServiceError(..))
import           Network.HTTP.Types (Status(..))
import           Test.Hspec

import           Control.Monad.AWS.S3
import           Control.Monad.AWS.S3.Mock

spec :: Spec
spec = do

  context "s3GetObjectStream" $ do

    it "Should mock s3GetObjectStream correctly" $ do
      result <- evalMockS3T (mockGetObjectAnyOk chunks) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        sourceToList $ s3GetObject (BucketName "My bucket") (ObjectKey "Key") Nothing
      result `shouldBe` chunks

    it "Should record mock s3GetObjectStream call" $ do
      let bucket = BucketName "My bucket"
          key = ObjectKey "Key"
      calls <- execMockS3T (mockGetObjectAnyOk chunks) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        sourceToList $ s3GetObject bucket key Nothing
      calls `shouldBe` [GetObject bucket key Nothing]

    it "Should record mock s3GetObjectStream call with version" $ do
      let bucket = BucketName "My bucket"
          key = ObjectKey "Key"
      calls <- execMockS3T (mockGetObjectAnyOk chunks) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        sourceToList $ s3GetObject bucket key (Just "0")
      calls `shouldBe` [GetObject bucket key (Just "0")]

  context "s3SaveObject" $ do

    it "Should mock s3SaveObject correctly" $ do
      result <- evalMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        s3SaveObject (BucketName "My bucket") (ObjectKey "Key") "/path/to/save.to" Nothing
      result `shouldBe` ()

    it "Should record mock s3SaveObject call" $ do
      let bucket = BucketName "My bucket"
          key = ObjectKey "Key"
          filePath = "/path/to/save.to"
      calls <- execMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        s3SaveObject bucket key filePath Nothing
      calls `shouldBe` [SaveObject bucket key filePath Nothing]

    it "Should record and mock multiple s3SaveObject calls correctly" $ do
      let bucket = BucketName "My super awesome bucket"
          key1 = ObjectKey "Key1"
          key2 = ObjectKey "Key2"
          filePath1 = "path/to/file1.csv"
          filePath2 = "path/to/file2.csv"
      calls <- execMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $ do
        s3SaveObject bucket key1 filePath1 Nothing
        s3SaveObject bucket key2 filePath2 Nothing
      calls `shouldBe` [ SaveObject bucket key1 filePath1 Nothing, SaveObject bucket key2 filePath2 Nothing ]

    it "Should record and mock multiple s3SaveObject calls with versions correctly" $ do
      let bucket = BucketName "My super awesome bucket"
          key1 = ObjectKey "Key1"
          key2 = ObjectKey "Key2"
          filePath1 = "path/to/file1.csv"
          filePath2 = "path/to/file2.csv"
      calls <- execMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $ do
        s3SaveObject bucket key1 filePath1 (Just "1")
        s3SaveObject bucket key2 filePath2 (Just "0")
      calls `shouldBe` [ SaveObject bucket key1 filePath1 (Just "1"), SaveObject bucket key2 filePath2 (Just "0") ]

  context "s3ListObjectVersions" $ do

    it "Should mock s3ListObjectVersions correctly" $ do
      result <- evalMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        s3ListObjectVersions (BucketName "My bucket") (ObjectKey "Key") 1
      result `shouldBe` Nothing

    it "Should record mock s3ListObjectVersions call" $ do
      let bucket = BucketName "My bucket"
          key = ObjectKey "Key"
      calls <- execMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        s3ListObjectVersions bucket key 1
      calls `shouldBe` [ListObjectVersions bucket key 1]


    it "Should mock s3ListObjectVersions call if there are versions" $ do
      let bucket = BucketName "My bucket"
          key = ObjectKey "Key"
          objectVersion1 = ObjectVersion True "A Version 1"
          objectVersion2 = ObjectVersion True "A Version 2"
      (versions, calls) <- runMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk [objectVersion1, objectVersion2]) mockSaveObjectAnyOk mockPutObjectAnyOk $
        s3ListObjectVersions bucket key 2
      calls `shouldBe` [ListObjectVersions bucket key 2]
      versions `shouldBe` (Just [objectVersion1, objectVersion2])

  context "s3PutObject" $ do

    it "Should mock s3PutObject correctly" $ do
      result <- evalMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        s3PutObject (BucketName "My bucket") (ObjectKey "Key") ("File bytes" :: ByteString)
      result `shouldBe` ()

    it "Should record mock s3PutObject call" $ do
      let bucket = BucketName "My bucket"
          key = ObjectKey "Key"
          object = "File bytes" :: ByteString
          hashed = show $ toHashed object
      calls <- execMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $
        s3PutObject bucket key object
      calls `shouldBe` [PutObject bucket key hashed "0"]

    it "Should record and mock multiple s3PutObject calls correctly" $ do
      let bucket = BucketName "My bucket"
          key1 = ObjectKey "Key1"
          key2 = ObjectKey "Key2"
          object1 = "File bytes" :: ByteString
          object2 = "File bytes" :: ByteString
          hashed1 = show $ toHashed object1
          hashed2 = show $ toHashed object2
      calls <- execMockS3T (mockGetObjectAnyOk []) (mockListObjectVersionsOk []) mockSaveObjectAnyOk mockPutObjectAnyOk $ do
        s3PutObject bucket key1 object1
        s3PutObject bucket key2 object2
      calls `shouldBe` [ PutObject bucket key1 hashed1 "0", PutObject bucket key2 hashed2 "1"]

  context "handle404" $ do

    it "Should return Just when no error" $ do
      result <- handle404 $ sourceToList source
      result `shouldBe` Just chunks

    it "Should return Nothing when 404 error" $ do
      let status404 = Status 404 "Missing file"
          error404 = ServiceError $ ServiceError' "S3"  status404 [] "404" Nothing Nothing
          noSource = throwM error404 :: MonadThrow m => ConduitT () ByteString m ()
      result <- handle404 $ sourceToList noSource
      result `shouldBe` Nothing

source :: Monad m => ConduitT () ByteString m ()
source = sourceList chunks

chunks :: [ByteString]
chunks = ["Hello", "my", "name", "is", "Morti"]
