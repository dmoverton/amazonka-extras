{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Network.AWS.DynamoDB.ExtraSpec (spec) where

import           Control.Lens ((&), (.~), (?~))
import           Control.Retry (constantDelay, limitRetries)
import           Data.Aeson (eitherDecode, encode)
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.Text1
                  (FromText1(..), Text1, ToText1(..), fromText1, fromText1Error, takeText1)
import           Data.Thyme (UTCTime)
import qualified Network.AWS.DynamoDB as Ddb
import           Test.Hspec
import           Test.Hspec.QuickCheck (prop)
import           Test.QuickCheck
import           Test.QuickCheck.Instances ()

import           Network.AWS.DynamoDB.Extra

spec :: Spec
spec = describe "Dynamo" $ do

  context "DynamoAddResult" $ do

    prop "to/fromText round trippable" $
      \(result :: DynamoAddResult) ->
        fromText1 (toText1 result) `shouldBe` Right result

    prop "To/FromJSON round trips" $ \(result :: DynamoAddResult) ->
      eitherDecode (encode result) `shouldBe` Right result

  context "DynamoUpdateIfUnchangedResult" $ do

    prop "to/fromText round trippable" $
      \(result :: DynamoUpdateIfUnchangedResult) ->
        fromText1 (toText1 result) `shouldBe` Right result

    prop "To/FromJSON round trips" $ \(result :: DynamoUpdateIfUnchangedResult) ->
      eitherDecode (encode result) `shouldBe` Right result

  context "buildUpdateExpression" $ do

    it "Multiple sets and removes" $ do
      let props =
            [ ("TestProp1", toAttributeValue ("value1" :: Text1))
            , ("TestProp2", toAttributeValue ("value2" :: Text1))
            , ("TestProp3", Ddb.attributeValue & Ddb.avNULL ?~ True)
            , ("TestProp4", Ddb.attributeValue & Ddb.avNULL ?~ True) ]
      buildUpdateExpression [] props `shouldBe`
        "SET #TestProp2 = :TestProp2, #TestProp1 = :TestProp1 REMOVE #TestProp4, #TestProp3"

    it "Single set and remove" $ do
      let props =
            [ ("TestProp1", toAttributeValue ("value1" :: Text1))
            , ("TestProp3", Ddb.attributeValue & Ddb.avNULL ?~ True) ]
      buildUpdateExpression [] props `shouldBe`
       "SET #TestProp1 = :TestProp1 REMOVE #TestProp3"

    it "Sets but no removes" $ do
      let props =
            [ ("TestProp1", toAttributeValue ("value1" :: Text1))
            , ("TestProp2", toAttributeValue ("value2" :: Text1)) ]
      buildUpdateExpression [] props `shouldBe`
       "SET #TestProp2 = :TestProp2, #TestProp1 = :TestProp1"

    it "Removes but no sets" $ do
      let props =
            [ ("TestProp3", Ddb.attributeValue & Ddb.avNULL ?~ True)
            , ("TestProp4", Ddb.attributeValue & Ddb.avNULL ?~ True) ]
      buildUpdateExpression [] props `shouldBe`
        "REMOVE #TestProp4, #TestProp3"

    it "Removes but no sets" $ do
      let props =
            [ ("TestProp3", Ddb.attributeValue & Ddb.avNULL ?~ True)
            , ("TestProp4", Ddb.attributeValue & Ddb.avNULL ?~ True) ]
      buildUpdateExpression [] props `shouldBe`
        "REMOVE #TestProp4, #TestProp3"

    it "Sets with values to not update" $ do
      let props =
            [ ("TestProp1", toAttributeValue ("value1" :: Text1))
            , ("TestProp2", toAttributeValue ("value2" :: Text1))
            , ("TestProp3", toAttributeValue ("value3" :: Text1))
            , ("TestProp4", toAttributeValue ("value4" :: Text1)) ]
      buildUpdateExpression ["TestProp1", "TestProp3"] props `shouldBe`
       "SET #TestProp4 = :TestProp4, \
           \#TestProp3 = if_not_exists(#TestProp3, :TestProp3), \
           \#TestProp2 = :TestProp2, \
           \#TestProp1 = if_not_exists(#TestProp1, :TestProp1)"

  context "buildExpressionAttributeNames" $

    it "Generates the expression names correctly" $
      let props =
            [ ("TestProp1", toAttributeValue ("value1" :: Text1))
            , ("TestProp2", Ddb.attributeValue & Ddb.avNULL ?~ True)
            , ("TestProp3", toAttributeValue ("value3" :: Text1)) ]
          expected =
            [ ("#TestProp1", "TestProp1")
            , ("#TestProp2", "TestProp2")
            , ("#TestProp3", "TestProp3") ]
      in buildExpressionAttributeNames props `shouldBe` expected

  context "buildExpressionAttributeValues" $

    it "Generates the expression names correctly" $
      let props =
            [ ("TestProp1", toAttributeValue ("value1" :: Text1))
            , ("TestProp2", Ddb.attributeValue & Ddb.avNULL ?~ True)
            , ("TestProp3", toAttributeValue ("value3" :: Text1)) ]
          expected =
            [ (":TestProp1", toAttributeValue ("value1" :: Text1))
            , (":TestProp3", toAttributeValue ("value3" :: Text1)) ]
      in buildExpressionAttributeValues props `shouldBe` expected

  context "buildFilteredExpressionAttributeValues" $

    it "Should only return attributes for the required keys" $
      let props =
            [ ("TestProp1", toAttributeValue ("value1" :: Text1))
            , ("TestProp2", toAttributeValue ("value2" :: Text1))
            , ("TestProp3", Ddb.attributeValue & Ddb.avNULL ?~ True) ]
          expected =
            [ (":TestProp1", toAttributeValue ("value1" :: Text1)) ]
      in buildFilteredExpressionAttributeValues ["TestProp1", "TestProp3"] props
        `shouldBe` expected

  context "buildFilteredExpressionAttributeValues'" $

    it "Should only return attributes for the required keys" $
      let bar@Bar{..} = Bar (BarName "Mos Eisley") Nothing [Luke, ObiWanKenobi] Nothing
          expected =
            [ (":Name", toAttributeValue _bName) ]
      in buildFilteredExpressionAttributeValues' ["Name", "Song"] bar `shouldBe` expected

  context "expressionToCondition" $ do

    let siteId = 1234 :: Int
        siteId1 = 1111 :: Int
        siteId2 = 2222 :: Int
        siteId3 = 3333 :: Int
        jobId = "abcd" :: Text1

    it "Eq" $
      expressionToCondition ("SiteId" `Eq` siteId) `shouldBe`
        Condition "#SiteId = :SiteId" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "Ne" $
      expressionToCondition ("SiteId" `Ne` siteId) `shouldBe`
        Condition "#SiteId <> :SiteId" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "Lt" $
      expressionToCondition ("SiteId" `Lt` siteId) `shouldBe`
        Condition "#SiteId < :SiteId" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "Le" $
      expressionToCondition ("SiteId" `Le` siteId) `shouldBe`
        Condition "#SiteId <= :SiteId" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "Gt" $
      expressionToCondition ("SiteId" `Gt` siteId) `shouldBe`
        Condition "#SiteId > :SiteId" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "Ge" $
      expressionToCondition ("SiteId" `Ge` siteId) `shouldBe`
        Condition "#SiteId >= :SiteId" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "Between" $
      expressionToCondition (Between "SiteId" siteId1 siteId2) `shouldBe`
        Condition
          "#SiteId BETWEEN :SiteId_Min AND :SiteId_Max"
          [("#SiteId", "SiteId")]
          [":SiteId_Min" ..= siteId1, ":SiteId_Max" ..= siteId2]

    it "In" $
      expressionToCondition ("SiteId" `In` [siteId1, siteId2, siteId3]) `shouldBe`
        Condition
          "#SiteId IN ( :SiteId_1, :SiteId_2, :SiteId_3 )"
          [("#SiteId", "SiteId")]
          [":SiteId_1" ..= siteId1, ":SiteId_2" ..= siteId2, ":SiteId_3" ..= siteId3]

    it "AttributeExists" $
      expressionToCondition (AttributeExists "SiteId") `shouldBe`
        Condition "attribute_exists(#SiteId)" [("#SiteId", "SiteId")] mempty

    it "AttributeNotExists" $
      expressionToCondition (AttributeNotExists "SiteId") `shouldBe`
        Condition "attribute_not_exists(#SiteId)" [("#SiteId", "SiteId")] mempty

    it "AttributeType" $
      expressionToCondition ("SiteId" `AttributeType` DdbString) `shouldBe`
        Condition
          "attribute_type(#SiteId, :SiteId_Type)"
          [("#SiteId", "SiteId")]
          [":SiteId_Type" ..= DdbString]

    it "BeginsWith" $
      expressionToCondition ("JobId" `BeginsWith` jobId) `shouldBe`
        Condition "begins_with(#JobId, :JobId_Begins)"
        [("#JobId", "JobId")]
        [":JobId_Begins" ..= jobId]

    it "Contains" $
      expressionToCondition ("SiteId" `Contains` SContains "23") `shouldBe`
        Condition "contains(#SiteId, :SiteId_Contains)"
        [("#SiteId", "SiteId")]
        [":SiteId_Contains" ..= SContains "23"]

    it "Size" $
      expressionToCondition (Size "SiteId") `shouldBe`
        Condition "size(#SiteId)" [("#SiteId", "SiteId")] mempty

    it "And" $
      expressionToCondition (("SiteId" `Eq` siteId) `And` ("JobId" `Eq` jobId))
      `shouldBe`
        Condition
          "#SiteId = :SiteId AND #JobId = :JobId"
          [("#SiteId", "SiteId"), ("#JobId", "JobId")]
          [":SiteId" ..= siteId, ":JobId" ..= jobId]

    it "Or" $
      expressionToCondition (("SiteId" `Eq` siteId) `Or` ("JobId" `Eq` jobId))
      `shouldBe`
        Condition
          "#SiteId = :SiteId OR #JobId = :JobId"
          [("#SiteId", "SiteId"), ("#JobId", "JobId")]
          [":SiteId" ..= siteId, ":JobId" ..= jobId]

    it "Not" $
      expressionToCondition (Not $ "SiteId" `Eq` siteId) `shouldBe`
        Condition "NOT #SiteId = :SiteId" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "Parens" $
      expressionToCondition (Parens $ "SiteId" `Eq` siteId) `shouldBe`
        Condition "( #SiteId = :SiteId )" [("#SiteId", "SiteId")] [":SiteId" ..= siteId]

    it "foldExpr" $ do
      let condition = expressionToCondition <$> foldExpr And [ "SiteId"    `Lt` siteId
                                                             , "JobId"     `Eq` jobId
                                                             , "Reference" `Eq` BarName "Ref" ]
      condition `shouldBe` Just
        (Condition
          "#SiteId < :SiteId AND #JobId = :JobId AND #Reference = :Reference"
          [("#SiteId", "SiteId"), ("#JobId", "JobId"), ("#Reference", "Reference")]
          [":SiteId" ..= siteId, ":Reference" ..= BarName "Ref", ":JobId" ..= jobId])

    it "foldExpr'" $ do
      let condition = expressionToCondition $ foldExpr' Or [ "SiteId"    `Lt` siteId
                                                           , "JobId"     `Eq` jobId
                                                           , "Reference" `Eq` BarName "Ref" ]
      condition `shouldBe`
        Condition
          "#SiteId < :SiteId OR #JobId = :JobId OR #Reference = :Reference"
          [("#SiteId", "SiteId"), ("#JobId", "JobId"), ("#Reference", "Reference")]
          [":SiteId" ..= siteId, ":Reference" ..= BarName "Ref", ":JobId" ..= jobId]

    it "Should remove dots in path" $
      expressionToCondition (AttributeNotExists "Thing.OtherThing.SiteId") `shouldBe`
        Condition "attribute_not_exists(#Thing_OtherThing_SiteId)"
          [("#Thing_OtherThing_SiteId", "Thing.OtherThing.SiteId")] mempty

  context "keyDoesNotExistCondition" $ do

    it "Should work for single value keys" $ do
      let bar = Bar (BarName "Mos Eisley") (Just "Cantina Band") [Luke, ObiWanKenobi] Nothing
          _cExpression = "attribute_not_exists(#Name)"
          _cExpressionAttributeNames = [("#Name", "Name")]
          _cExpressionAttributeValues = mempty
      fmap expressionToCondition (keyDoesNotExistCondition bar) `shouldBe` Just Condition{..}

    it "Should work for multi value keys" $ do
      let lightSaber = LightSaber "Blue" ObiWanKenobi undefined
          _cExpression = "attribute_not_exists(#Jedi) AND attribute_not_exists(#DateAcquired)"
          _cExpressionAttributeNames = [("#Jedi", "Jedi"), ("#DateAcquired", "DateAcquired")]
          _cExpressionAttributeValues = mempty
      fmap expressionToCondition (keyDoesNotExistCondition lightSaber) `shouldBe` Just Condition{..}

  context "retryUnproccessedGets" $ do

    let
      makeUnique :: NonEmpty Bar -> NonEmpty Bar
      makeUnique bars = do
        let makeJobUnique bar i = bar { _bName = BarName (toText1 i) }
        NE.zipWith makeJobUnique bars $ NE.fromList [0 :: Int ..]

      processOneItem ::
        (Monad m, DynamoEntity a, Eq (Key a)) => NonEmpty a -> NonEmpty (Key a) -> m (DynamoBatchGetResponse a)
      processOneItem items (key :| otherKeys) =
        let proccessed = maybe mempty (pure . Right) $ List.find ((key ==) . getKey) $ NE.toList items
        in case NE.nonEmpty otherKeys of
          Nothing          -> pure $ BatchGetAllProcessed proccessed
          Just unprocessed -> pure $ BatchGetUnprocessed proccessed $ Right <$> unprocessed

      processOneItem' ::
        (Monad m, DynamoEntity a, Eq (Key a)) => NonEmpty a -> NonEmpty (Key a) -> m (DynamoBatchGetResponse a)
      processOneItem' items (key :| otherKeys) =
        let proccessed = maybe mempty (pure . Right) $ List.find ((key ==) . getKey) $ NE.toList items
        in case NE.nonEmpty otherKeys of
        Nothing -> pure $ BatchGetAllProcessed proccessed
        Just unprocessed -> pure $ BatchGetUnprocessed proccessed $ const (Left "Cannot Parse Key") <$> unprocessed

      processNoItems :: Monad m => NonEmpty (Key a) -> m (DynamoBatchGetResponse a)
      processNoItems = pure . BatchGetUnprocessed mempty . fmap Right

    prop "Should process all items if they all process first time" $
      \(items :: NonEmpty Bar) -> do
        let uniqueItems = makeUnique items
            keys = getKey <$> uniqueItems
            retryPolicy = constantDelay 0
            allProcessSuccessfully _ = pure $ BatchGetAllProcessed $ NE.toList $ Right <$> uniqueItems
        result <- retryUnproccessedGets @Bar retryPolicy allProcessSuccessfully keys
        result `shouldBe` NE.toList (Right <$> uniqueItems)

    prop "Should process all items eventually without limited retires" $
      \(items :: NonEmpty Bar) -> do
        let uniqueItems = makeUnique items
            keys = getKey <$> uniqueItems
            retryPolicy = constantDelay 0
        result <- retryUnproccessedGets @Bar retryPolicy (processOneItem uniqueItems) keys
        result `shouldBe` NE.toList (Right <$> uniqueItems)

    prop "Should fail if we run out of retires" $
      \(items :: NonEmpty Bar) -> NE.length items > 1 ==> do
        let uniqueItems = makeUnique items
            keys = getKey <$> uniqueItems
            retryPolicy = constantDelay 0 <> limitRetries (NE.length uniqueItems - 2)
            unprocessedItemsException unprocessedItems = do
              let expected = UnprocessedItems (show <$> keys) [show $ NE.last keys]
              unprocessedItems == expected
        retryUnproccessedGets @Bar retryPolicy (processOneItem uniqueItems) keys
          `shouldThrow` unprocessedItemsException

    prop "Should fail if no items are processed" $
      \(items :: NonEmpty Bar) -> do
        let uniqueItems = makeUnique items
            keys = getKey <$> uniqueItems
            retryPolicy = constantDelay 0 <> limitRetries (NE.length uniqueItems)
            unprocessedItemsException unprocessedItems = do
              let expected = UnprocessedItems (show <$> keys) (show . getKey <$> uniqueItems)
              unprocessedItems == expected
        retryUnproccessedGets @Bar retryPolicy processNoItems keys
          `shouldThrow` unprocessedItemsException

    prop "Should fail if we cannot convert unprocessed keys" $
      \(items :: NonEmpty Bar) -> NE.length items > 1 ==> do
        let uniqueItems = makeUnique items
            keys = getKey <$> uniqueItems
            retryPolicy = constantDelay 0
            unparseableUnprocessedItemsException unparseableUnprocessedItems = do
              let expected = UnparseableUnprocessedItems
                    (show <$> keys)
                    ("Cannot Parse Key" <$ NE.fromList (NE.tail keys))
                    []
              unparseableUnprocessedItems == expected
        retryUnproccessedGets @Bar retryPolicy (processOneItem' uniqueItems) keys
          `shouldThrow` unparseableUnprocessedItemsException

  context "retryUnproccessedWrites" $ do

    let
      processOneItem :: Monad m => NonEmpty a -> m (DynamoBatchWriteResponse a)
      processOneItem keys = case NE.nonEmpty $ NE.tail keys of
        Nothing          -> pure $ BatchWriteAllProcessed
        Just unprocessed -> pure $ BatchWriteUnprocessed $ Right <$> unprocessed

      processOneItem' :: Monad m => NonEmpty a -> m (DynamoBatchWriteResponse a)
      processOneItem' keys = case NE.nonEmpty $ NE.tail keys of
        Nothing -> pure $ BatchWriteAllProcessed
        Just unprocessed -> pure $ BatchWriteUnprocessed $ const (Left "Cannot Parse Key") <$> unprocessed

      processNoItems :: Monad m => NonEmpty a -> m (DynamoBatchWriteResponse a)
      processNoItems = pure . BatchWriteUnprocessed . fmap Right

    prop "Should process all items if they all process first time" $
      \(items :: NonEmpty Bar) -> do
        let keys = getKey <$> items
            retryPolicy = constantDelay 0
            allProcessSuccessfully _ = pure BatchWriteAllProcessed
        result <- retryUnproccessedWrites @BarName retryPolicy allProcessSuccessfully keys
        result `shouldBe` ()

    prop "Should process all items eventually without limited retires" $
      \(items :: NonEmpty Bar) -> do
        let keys = getKey <$> items
            retryPolicy = constantDelay 0
        result <- retryUnproccessedWrites @BarName retryPolicy processOneItem keys
        result `shouldBe` ()

    prop "Should fail if we run out of retires" $
      \(items :: NonEmpty Bar) -> NE.length items > 1 ==> do
        let keys = getKey <$> items
            retryPolicy = constantDelay 0 <> limitRetries (NE.length items - 2)
            unprocessedItemsException unprocessedItems = do
              let expected = UnprocessedItems (show <$> keys) $ [show $ NE.last keys]
              unprocessedItems == expected
        retryUnproccessedWrites @BarName retryPolicy processOneItem keys
          `shouldThrow` unprocessedItemsException

    prop "Should fail if no items are processed" $
      \(items :: NonEmpty Bar) -> do
        let keys = getKey <$> items
            retryPolicy = constantDelay 0 <> limitRetries (NE.length items)
            unprocessedItemsException unprocessedItems = do
              let expected = UnprocessedItems (show <$> keys) (show . getKey <$> items)
              unprocessedItems == expected
        retryUnproccessedWrites @BarName retryPolicy processNoItems keys
          `shouldThrow` unprocessedItemsException

    prop "Should fail if we cannot convert unprocessed keys" $
      \(items :: NonEmpty Bar) -> NE.length items > 1 ==> do
        let keys = getKey <$> items
            retryPolicy = constantDelay 0
            unparseableUnprocessedItemsException unparseableUnprocessedItems = do
              let expected = UnparseableUnprocessedItems
                    (show <$> keys)
                    ("Cannot Parse Key" <$ NE.fromList (NE.tail keys))
                    []
              unparseableUnprocessedItems == expected
        retryUnproccessedWrites @BarName retryPolicy processOneItem' keys
          `shouldThrow` unparseableUnprocessedItemsException

  context "DynamoEntity" $ do

    prop "to/fromAttributeValues" $
      \(bar@Bar{..} :: Bar) -> do
        parseEither fromAttributeValues (toAttributeValues bar) `shouldBe` pure bar
        parseEither fromAttributeValues' (toAttributeValues' bar) `shouldBe` pure bar

    it "Should produce correct to dynamo result" $ do
      let bar = Bar
            { _bName = BarName "Mos Eisley"
            , _bSong = Nothing
            , _bVisitedBy = []
            , _bDroidsAllowed = Nothing }
          expected = fromDynamoList
            [ ("Name"         , Ddb.attributeValue & Ddb.avS ?~ "Mos Eisley")
            , ("Song"         , Ddb.attributeValue & Ddb.avNULL ?~ True)
            , ("VisitedBy"    , Ddb.attributeValue & Ddb.avL .~ [])
            , ("DroidsAllowed", Ddb.attributeValue & Ddb.avNULL ?~ True) ]
      toAttributeValues bar `shouldBe` expected

    it "Should produce correct from dynamo result" $ do
      let bar = fromDynamoList
            [ ("Name"         , Ddb.attributeValue & Ddb.avS ?~ "Mos Eisley")
            , ("Song"         , Ddb.attributeValue & Ddb.avNULL ?~ True)
            , ("VisitedBy"    , Ddb.attributeValue & Ddb.avL .~ [Ddb.attributeValue & Ddb.avS ?~ "Luke"])
            , ("DroidsAllowed", Ddb.attributeValue & Ddb.avNULL ?~ True) ]
          expected = Bar
            { _bName = BarName "Mos Eisley"
            , _bSong = Nothing
            , _bVisitedBy = [Luke]
            , _bDroidsAllowed = Nothing }
      parseEither fromAttributeValues bar `shouldBe` pure expected

    it "Should load with missing keys" $ do
      let dynamo = fromDynamoList
            [ "Name" ..= BarName "Mos Eisley", "Song" ..= (Nothing :: Maybe Text1) ]
      parseEither fromAttributeValues dynamo `shouldBe`
        pure (Bar (BarName "Mos Eisley") Nothing mempty Nothing)

newtype BarName =
  BarName Text1
  deriving (Show, Eq, ToAttributeValue, FromAttributeValue, Arbitrary)

data Bar =
  Bar
    { _bName          :: BarName
    , _bSong          :: Maybe Text1
    , _bVisitedBy     :: [Jedi]
    , _bDroidsAllowed :: Maybe Bool }
  deriving (Eq, Show)

instance Arbitrary Bar where
  arbitrary =
    Bar
    <$> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

instance AttributeValues BarName where
  toAttributeValues name = fromDynamoList [ "Name" ..= name ]
  fromAttributeValues e = BarName <$> e ..: "Name"

instance AttributeValues Bar where
  toAttributeValues Bar{..} =
    fromDynamoList
      [ "Name"          ..= _bName
      , "Song"          ..= _bSong
      , "VisitedBy"     ..= _bVisitedBy
      , "DroidsAllowed" ..= _bDroidsAllowed ]

  fromAttributeValues e =
    Bar
      <$> e ..: "Name"
      <*> e ..? "Song"
      <*> e ..? "VisitedBy"
      <*> e ..? "DroidsAllowed"

instance DynamoEntity Bar where
  type Key Bar = BarName
  getKey Bar{..} = _bName

data LightSaber =
  LightSaber
    { _lsColour       :: Text1
    , _lsJedi         :: Jedi
    , _lsDateAcquired :: UTCTime }

instance AttributeValues (Jedi, UTCTime) where
  toAttributeValues (jedi, dateAcquired) =
    fromDynamoList
      [ "Jedi"         ..= jedi
      , "DateAcquired" ..= dateAcquired ]

  fromAttributeValues e =
    (,)
      <$> e ..: "Jedi"
      <*> e ..: "DateAcquired"

instance AttributeValues LightSaber where
  toAttributeValues LightSaber{..} =
    fromDynamoList
      [ "Colour"       ..= _lsColour
      , "Jedi"         ..= _lsJedi
      , "DateAcquired" ..= _lsDateAcquired ]

  fromAttributeValues e =
    LightSaber
      <$> e ..: "Colour"
      <*> e ..: "Jedi"
      <*> e ..: "DateAcquired"

instance DynamoEntity LightSaber where
  type Key LightSaber = (Jedi, UTCTime)
  getKey LightSaber{..} = (_lsJedi, _lsDateAcquired)

data Jedi =
    ObiWanKenobi
  | Luke
  | Yoda
  deriving (Show, Eq, Bounded, Enum)

instance FromText1 Jedi where
  parser1 = takeText1 >>= \case
    "ObiWanKenobi" -> pure ObiWanKenobi
    "Luke"         -> pure Luke
    "Yoda"         -> pure Yoda
    e              -> fromText1Error $ "The force is not strong with you: '" <> e <> "'."

instance ToText1 Jedi where
  toText1 ObiWanKenobi = "ObiWanKenobi"
  toText1 Luke         = "Luke"
  toText1 Yoda         = "Yoda"

instance ToAttributeValue Jedi where
  toAttributeValue = toText1AttributeValue

instance FromAttributeValue Jedi where
  fromAttributeValue = fromText1AttributeValue

instance Arbitrary Jedi where
  arbitrary = arbitraryBoundedEnum
