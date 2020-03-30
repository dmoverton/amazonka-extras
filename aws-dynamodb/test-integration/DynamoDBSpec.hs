{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module DynamoDBSpec (spec) where

import           Control.Lens ((&), (.~))
import           Control.Monad (void)
import           Data.Conduit (sourceToList)
import           Data.Text1 (Text1)
import           Data.Thyme (UTCTime)
import           Test.Hspec (Spec, it, shouldBe, shouldMatchList)

import           Network.AWS.DynamoDB.AttributeValue
                  (AttributeValues(..), FromAttributeValue, ToAttributeValue, fromDynamoList,
                  (..:), (..=))
import           Network.AWS.DynamoDB.Extra
                  (DynamoAddResult(..), DynamoBatchGetResponse(..), DynamoBatchWriteResponse(..),
                  DynamoEntity(..), DynamoLookupResult(..), DynamoUpdateIfUnchangedResult(..),
                  Existing(..), defaultAdd, defaultDelete, defaultDeleteBatch, defaultGet,
                  defaultGetBatch, defaultPut, defaultPutBatch, defaultScan, defaultUpdate,
                  defaultUpdateIfUnchanged)
import           Test.Hspec.AWS.DynamoDB
                  (DynamoLocal(..), dcResetAfterEachTest, defaultConfig, withDynamoLocal)

-- | Before running these tests locally you must start the dynamo local docker container
--   `docker run -p 8000:8000 amazon/dynamodb-local`
spec :: Spec
spec = withDynamoLocal (defaultConfig & dcResetAfterEachTest .~ False) $ do

  it "Should not get when doesnt exist" $ \DynamoLocal{..} -> do
    result :: DynamoLookupResult TestRecord <- _dlRunAws $ defaultGet _dlTableName $ TestId 1111
    result `shouldBe` NotFound

  it "Should add when doesnt exist" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 1111) "Test" today 42
    result <- _dlRunAws $ defaultAdd _dlTableName record
    result `shouldBe` DynamoAdded

  it "Should get when exists" $ \DynamoLocal{..} -> do
    result <- _dlRunAws $ defaultGet _dlTableName $ TestId 1111
    result `shouldBe` Found (TestRecord (TestId 1111) "Test" today 42)

  it "Should fail to parse on get of wrong type" $ \DynamoLocal{..} -> do
    result :: DynamoLookupResult BrokenRecord <- _dlRunAws $ defaultGet _dlTableName $ TestId 1111
    result `shouldBe` ParseError "Error in $: key \"OnAndOn\" not present"

  it "Shouldnt add when already exists" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 1111) "Test" today 42
    result <- _dlRunAws $ defaultAdd _dlTableName record
    result `shouldBe` DynamoAlreadyExists

  it "Shouldnt add when already exists even if values are different" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 1111) "Testing" tomorrow 99
    result <- _dlRunAws $ defaultAdd _dlTableName record
    result `shouldBe` DynamoAlreadyExists

  it "Should put when doesnt exist" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 2222) "Test" today 42
    result <- _dlRunAws $ do
      defaultPut _dlTableName record
      defaultGet _dlTableName $ TestId 2222
    result `shouldBe` Found record

  it "Should put when already exists" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 2222) "Testing" tomorrow 99
    result <- _dlRunAws $ do
      defaultPut _dlTableName record
      defaultGet _dlTableName $ TestId 2222
    result `shouldBe` Found record

  it "Should put batch when doesnt exist" $ \DynamoLocal{..} -> do
    let recordA = TestRecord (TestId 1234) "Test" today 42
        recordB = TestRecord (TestId 2345) "Test" today 43
    result <- _dlRunAws $ do
      putResult <- defaultPutBatch _dlTableName $ [recordA, recordB]
      resultA <- defaultGet _dlTableName $ TestId 1234
      resultB <- defaultGet _dlTableName $ TestId 2345
      pure (putResult, resultA, resultB)
    result `shouldBe` (BatchWriteAllProcessed, Found recordA, Found recordB)

  it "Should put batch when already exists" $ \DynamoLocal{..} -> do
    let recordA = TestRecord (TestId 1234) "Test" tomorrow 99
        recordB = TestRecord (TestId 2345) "Test" tomorrow 100
    result <- _dlRunAws $ do
      putResult <- defaultPutBatch _dlTableName $ [recordA, recordB]
      resultA <- defaultGet _dlTableName $ TestId 1234
      resultB <- defaultGet _dlTableName $ TestId 2345
      pure (putResult, resultA, resultB)
    result `shouldBe` (BatchWriteAllProcessed, Found recordA, Found recordB)

  it "Should scan all items" $ \DynamoLocal{..} -> do
    let record1 = TestRecord (TestId 1111) "Test" today 42
    let record2 = TestRecord (TestId 2222) "Testing" tomorrow 99
    let record3 = TestRecord (TestId 1234) "Test" tomorrow 99
    let record4 = TestRecord (TestId 2345) "Test" tomorrow 100
    result <- _dlRunAws $ sourceToList $ defaultScan _dlTableName Nothing
    result `shouldMatchList` [record1, record2, record3, record4]

  it "Should get multiple in one batch skipping keys not found" $ \DynamoLocal{..} -> do
    let record1 = TestRecord (TestId 1111) "Test" today 42
    let record2 = TestRecord (TestId 2222) "Testing" tomorrow 99
    result <- _dlRunAws $ defaultGetBatch _dlTableName [TestId 1111, TestId 2222, TestId 3333]
    result `shouldBe` BatchGetAllProcessed [Right record1, Right record2]

  it "Should get multiple in one batch when no keys found" $ \DynamoLocal{..} -> do
    result <- _dlRunAws $ defaultGetBatch @TestRecord _dlTableName $ [TestId 7777, TestId 8888, TestId 9999]
    result `shouldBe` BatchGetAllProcessed []

  it "Should delete when already exists" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 1111) "Test" today 42
    result <- _dlRunAws $ defaultDelete _dlTableName $ TestId 1111
    result `shouldBe` Found record

  it "Should fail to parse when deleting wrong type" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 1111) "Test" today 42
    result :: DynamoLookupResult BrokenRecord <- _dlRunAws $ do
      void $ defaultAdd _dlTableName record
      defaultDelete _dlTableName $ TestId 1111
    result `shouldBe` ParseError "Error in $: key \"OnAndOn\" not present"

  it "Should have deleted even when deleting wrong type" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 1111) "Test" today 42
    result :: DynamoLookupResult TestRecord <- _dlRunAws $ do
      void $ defaultAdd _dlTableName record
      _ :: DynamoLookupResult BrokenRecord <- defaultDelete _dlTableName $ TestId 1111
      defaultDelete _dlTableName $ TestId 1111
    result `shouldBe` NotFound

  it "Shouldnt delete when doesnt exist" $ \DynamoLocal{..} -> do
    result :: DynamoLookupResult TestRecord <- _dlRunAws $ defaultDelete _dlTableName $ TestId 1111
    result `shouldBe` NotFound

  it "Should delete multiple in one batch" $ \DynamoLocal{..} -> do
    result <- _dlRunAws $ do
      void $ defaultAdd _dlTableName $ TestRecord (TestId 3333) "Test 3" today 33
      void $ defaultAdd _dlTableName $ TestRecord (TestId 4444) "Test 4" today 44
      void $ defaultAdd _dlTableName $ TestRecord (TestId 5555) "Test 5" today 55
      void $ defaultAdd _dlTableName $ TestRecord (TestId 6666) "Test 6" today 66
      defaultDeleteBatch @TestRecord _dlTableName [TestId 3333, TestId 4444, TestId 5555]
    result `shouldBe` BatchWriteAllProcessed
    recordIds <- _dlRunAws $ sourceToList $ defaultScan _dlTableName Nothing
    (_trId <$> recordIds) `shouldMatchList` [TestId 6666, TestId 2222, TestId 1234, TestId 2345]

  it "Should update all values" $ \DynamoLocal{..} -> do
    let record = TestRecord (TestId 2222) "Test" today 42
    result <- _dlRunAws $ do
      defaultUpdate _dlTableName [] record
      defaultGet _dlTableName $ TestId 2222
    result `shouldBe` Found record

  it "Shouldnt update all values except the ones we specify" $ \DynamoLocal{..} -> do
    let newRecord = TestRecord (TestId 2222) "Test" tomorrow 150
        expectedRecord = TestRecord (TestId 2222) "Test" tomorrow 42
    result <- _dlRunAws $ do
      defaultUpdate _dlTableName ["Count"] newRecord
      defaultGet _dlTableName $ TestId 2222
    result `shouldBe` Found expectedRecord

  it "Should update if unchanged" $ \DynamoLocal{..} -> do
    let existing = Existing $ TestRecord (TestId 2222) "Test" tomorrow 42
        record = TestRecord (TestId 2222) "Testing" tomorrow 42
    result <- _dlRunAws $ defaultUpdateIfUnchanged _dlTableName existing record
    result `shouldBe` DynamoUpdateSuccessful

  it "Should not update if unchanged" $ \DynamoLocal{..} -> do
    let existing = Existing $ TestRecord (TestId 2222) "Test" tomorrow 42
        record = TestRecord (TestId 2222) "Testing" tomorrow 42
    result <- _dlRunAws $ defaultUpdateIfUnchanged _dlTableName existing record
    result `shouldBe` DynamoUpdateDirtyRead

newtype TestId = TestId Int deriving (Show, Eq, ToAttributeValue, FromAttributeValue)

data TestRecord =
  TestRecord
    { _trId    :: TestId
    , _trName  :: Text1
    , _trDate  :: UTCTime
    , _trCount :: Int }
  deriving (Show, Eq)

instance AttributeValues TestId where
  toAttributeValues testId = fromDynamoList [ "Id" ..= testId ]
  fromAttributeValues e = TestId <$> e ..: "Id"

instance AttributeValues TestRecord where
  toAttributeValues TestRecord{..} =
    fromDynamoList
      [ "Id"    ..= _trId
      , "Name"  ..= _trName
      , "Date"  ..= _trDate
      , "Count" ..= _trCount ]

  fromAttributeValues e =
    TestRecord
      <$> e ..: "Id"
      <*> e ..: "Name"
      <*> e ..: "Date"
      <*> e ..: "Count"

instance DynamoEntity TestRecord where
  type Key TestRecord = TestId
  getKey TestRecord{..} = _trId


data BrokenRecord =
  BrokenRecord
    { _brId      :: TestId
    , _brOnAndOn :: Text1 }
  deriving (Show, Eq)

instance AttributeValues BrokenRecord where
  toAttributeValues BrokenRecord{..} =
    fromDynamoList
      [ "Id"       ..= _brId
      , "OnAndOn"  ..= _brOnAndOn ]

  fromAttributeValues e =
    BrokenRecord
      <$> e ..: "Id"
      <*> e ..: "OnAndOn"

instance DynamoEntity BrokenRecord where
  type Key BrokenRecord = TestId
  getKey BrokenRecord{..} = _brId

today :: UTCTime
today = "2018-11-21T00:00:00.000Z"

tomorrow :: UTCTime
tomorrow = "2018-11-22T00:00:00.000Z"
