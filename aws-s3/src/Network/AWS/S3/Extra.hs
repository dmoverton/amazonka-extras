{-# OPTIONS_GHC -Wno-orphans #-}

module Network.AWS.S3.Extra where

import           Data.Aeson (FromJSON(..), ToJSON(..))
import           Network.AWS.S3 (BucketName(..), ObjectKey(..))
import           Test.QuickCheck (Arbitrary(..))
import           Test.QuickCheck.Instances ()

instance Arbitrary BucketName where
  arbitrary = BucketName <$> arbitrary

instance ToJSON BucketName where
  toJSON (BucketName bucketName) = toJSON bucketName

instance Arbitrary ObjectKey where
  arbitrary = ObjectKey <$> arbitrary

instance ToJSON ObjectKey where
  toJSON (ObjectKey objectKey) = toJSON objectKey

instance FromJSON ObjectKey where
  parseJSON = fmap ObjectKey . parseJSON
