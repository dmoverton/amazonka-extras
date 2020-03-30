{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Network.AWS.DynamoDB.AttributeValueSpec (spec) where

import           Control.Applicative ((<|>))
import           Control.Lens ((&), (.~), (?~))
import           Control.Newtype.Generics (Newtype(..))
import           Data.Aeson (Value(..), object, (.=))
import           Data.Bifunctor (bimap)
import           Data.ByteString (ByteString)
import           Data.Fixed (E0, E1, E12, E2, E3, E6, E9, Fixed, HasResolution)
import           Data.Hashable (Hashable)
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import           Data.Int (Int64)
import           Data.IntSet (IntSet)
import qualified Data.IntSet as IS
import           Data.List (sort)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Scientific (Scientific)
import           Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as Text
import           Data.Text1
                  (AsText1(..), FromText1(..), Text1, ToText1(..), fromText1Error, takeText1,
                  text1ToText, toText)
import qualified Data.Thyme as Thyme
import           Data.UUID.Types (UUID)
import           GHC.Generics (Generic)
import           Network.AWS.Data.Numeric (Nat(..))
import qualified Network.AWS.DynamoDB as Ddb
import           Numeric.Natural (Natural)
import           System.Locale (defaultTimeLocale)
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck hiding (Fixed)
import           Test.QuickCheck.Instances ()

import           Network.AWS.DynamoDB.AttributeValue
import           Network.AWS.DynamoDB.Extra (DynamoEntity(..), Key)

spec :: Spec
spec = describe "AttributeValue" $ do

  context "To/FromAttributeValue" $ do

    prop "Should convert ByteString" $
      \(byteString :: ByteString) ->
        parseAttributeValue (toAttributeValue byteString) `shouldBe` pure byteString

    prop "Should assign non-empty ByteString to correct attribute value type" $
      \(byteString :: ByteString) -> byteString /= mempty ==> do
        let expected = Ddb.attributeValue & Ddb.avB ?~ byteString
        toAttributeValue byteString `shouldBe` expected

    it "Should assign empty ByteString to correct attribute value type" $ do
      let byteString = "" :: ByteString
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue byteString `shouldBe` expected

    prop "Should convert ByteString from correct attribute value type" $
      \(byteString :: ByteString) -> do
        let av = Ddb.attributeValue & Ddb.avB ?~ byteString
        parseAttributeValue av `shouldBe` pure byteString

    -- prop "Should convert Thyme.UTCTime" $
    --   \(utcTime :: Thyme.UTCTime) ->
    --     parseAttributeValue (toAttributeValue utcTime) `shouldBe` pure utcTime
    --
    -- it "Should assign Thyme.UTCTime to correct attibute value type" $ do
    --   let utcTime = Thyme.readTime defaultTimeLocale "%Y-%m-%d" "2016-11-08" :: Thyme.UTCTime
    --       expected = Ddb.attributeValue & Ddb.avS ?~ "2016-11-08T00:00:00Z"
    --   toAttributeValue utcTime `shouldBe` expected
    --
    -- it "Should convert Thyme.UTCTime from correct attibute value type" $ do
    --   let utcTime = Thyme.readTime defaultTimeLocale "%Y-%m-%d" "2016-11-08" :: Thyme.UTCTime
    --       av = Ddb.attributeValue & Ddb.avS ?~ "2016-11-08T00:00:00Z"
    --   parseAttributeValue av `shouldBe` pure utcTime

    prop "Should convert UUID" $
      \(uuid :: UUID) ->
        parseAttributeValue (toAttributeValue uuid) `shouldBe` pure uuid

    it "Should assign UUID to correct attibute value type" $ do
      let uuid = read "a5ca8566-d9c5-4835-99c8-e1f13e73b5e2" :: UUID
          expected = Ddb.attributeValue & Ddb.avS ?~ "a5ca8566-d9c5-4835-99c8-e1f13e73b5e2"
      toAttributeValue uuid `shouldBe` expected

    it "Should convert UUID from correct attibute value type" $ do
      let uuid = read "a5ca8566-d9c5-4835-99c8-e1f13e73b5e2" :: UUID
          av = Ddb.attributeValue & Ddb.avS ?~ "a5ca8566-d9c5-4835-99c8-e1f13e73b5e2"
      parseAttributeValue av `shouldBe` pure uuid

    prop "Should convert Bool" $
      \(bool :: Bool) ->
        parseAttributeValue (toAttributeValue bool) `shouldBe` pure bool

    prop "Should assign Bool to correct attribute value type" $
      \(bool :: Bool) -> do
        let expected = Ddb.attributeValue & Ddb.avBOOL ?~ bool
        toAttributeValue bool `shouldBe` expected

    prop "Should convert Bool from correct attribute value type" $
      \(bool :: Bool) -> do
        let av = Ddb.attributeValue & Ddb.avBOOL ?~ bool
        parseAttributeValue av `shouldBe` pure bool

    prop "Should convert Text1" $
      \(text :: Text1) ->
        parseAttributeValue (toAttributeValue text) `shouldBe` pure text

    prop "Should assign Text1 to correct attribute value type" $
      \(text :: Text1) -> do
        let expected = Ddb.attributeValue & Ddb.avS ?~ text1ToText text
        toAttributeValue text `shouldBe` expected

    prop "Should convert Maybe Text1" $
      \(text :: Maybe Text1) ->
        parseAttributeValue (toAttributeValue text) `shouldBe` pure text

    prop "Should assign Maybe Text1 to correct attribute value type when Just" $
      \(text :: Text1) -> do
        let expected = Ddb.attributeValue & Ddb.avS ?~ text1ToText text
        toAttributeValue (Just text) `shouldBe` expected

    it "Should assign Maybe Text1 to correct attribute value type when Nothing" $ do
        let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
        toAttributeValue (Nothing :: Maybe Text1) `shouldBe` expected

    prop "Should convert Double" $
      \(double :: Double) ->
        parseAttributeValue (toAttributeValue double) `shouldBe` pure double

    prop "Should assign Double to correct attribute value type" $
      \(double :: Double) -> do
        let expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 double)
        toAttributeValue double `shouldBe` expected

    prop "Should convert Double from correct attribute value type" $
      \(double :: Double) -> do
        let av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 double)
        parseAttributeValue av `shouldBe` pure double

    prop "Should convert Int" $
      \(int :: Int) ->
        parseAttributeValue (toAttributeValue int) `shouldBe` pure int

    prop "Should assign Int to correct attribute value type" $
      \(int :: Int) -> do
        let expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 int)
        toAttributeValue int `shouldBe` expected

    prop "Should convert Int from correct attribute value type" $
      \(int :: Int) -> do
        let av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 int)
        parseAttributeValue av `shouldBe` pure int

    prop "Should convert Int64" $
      \(int :: Int64) ->
        parseAttributeValue (toAttributeValue int) `shouldBe` pure int

    prop "Should assign Int64 to correct attribute value type" $
      \(int :: Int64) -> do
        let expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 int)
        toAttributeValue int `shouldBe` expected

    prop "Should convert Int64 from correct attribute value type" $
      \(int :: Int64) -> do
        let av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 int)
        parseAttributeValue av `shouldBe` pure int

    prop "Should convert Integer" $
      \(int :: Integer) ->
        parseAttributeValue (toAttributeValue int) `shouldBe` pure int

    prop "Should assign Integer to correct attribute value type" $
      \(int :: Integer) -> do
        let expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 int)
        toAttributeValue int `shouldBe` expected

    prop "Should convert Integer from correct attribute value type" $
      \(int :: Integer) -> do
        let av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 int)
        parseAttributeValue av `shouldBe` pure int

    prop "Should convert Natural" $
      \(nat :: Natural) ->
        parseAttributeValue (toAttributeValue nat) `shouldBe` pure nat

    prop "Should assign Natural to correct attribute value type" $
      \(nat :: Natural) -> do
        let expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 nat)
        toAttributeValue nat `shouldBe` expected

    prop "Should convert Natural from correct attribute value type" $
      \(nat :: Natural) -> do
        let av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 nat)
        parseAttributeValue av `shouldBe` pure nat

    prop "Should convert Nat" $
      \(natural :: Natural) -> do
        let nat = Nat natural
        parseAttributeValue (toAttributeValue nat) `shouldBe` pure nat

    prop "Should assign Nat to correct attribute value type" $
      \(natural :: Natural) -> do
        let nat = Nat natural
            expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 nat)
        toAttributeValue nat `shouldBe` expected

    prop "Should convert Nat from correct attribute value type" $
      \(natural :: Natural) -> do
        let nat = Nat natural
            av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 nat)
        parseAttributeValue av `shouldBe` pure nat

    prop "Should convert Scientific" $
      \(scientific :: Scientific) ->
        parseAttributeValue (toAttributeValue scientific) `shouldBe` pure scientific

    prop "Should assign Scientific to correct attribute value type" $
      \(scientific :: Scientific) -> do
        let expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 scientific)
        toAttributeValue scientific `shouldBe` expected

    prop "Should convert Scientific from correct attribute value type" $
      \(scientific :: Scientific) -> do
        let av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 scientific)
        parseAttributeValue av `shouldBe` pure scientific

    let
      shouldConvertFixed :: HasResolution a => Fixed a -> Expectation
      shouldConvertFixed fixed =
        parseAttributeValue (toAttributeValue fixed) `shouldBe` pure fixed

      shouldAssignFixedToCorrectType :: HasResolution a => Fixed a -> Expectation
      shouldAssignFixedToCorrectType fixed =
        let expected = Ddb.attributeValue & Ddb.avN ?~ Text.pack (show fixed)
        in toAttributeValue fixed `shouldBe` expected

      shouldConvertFixedFromCorrectType :: HasResolution a => Fixed a -> Expectation
      shouldConvertFixedFromCorrectType fixed =
        let av = Ddb.attributeValue & Ddb.avN ?~ Text.pack (show fixed)
        in parseAttributeValue av `shouldBe` pure fixed

    prop "Should convert Fixed E12" $ shouldConvertFixed @E12
    prop "Should convert Fixed E9" $ shouldConvertFixed @E9
    prop "Should convert Fixed E6" $ shouldConvertFixed @E6
    prop "Should convert Fixed E3" $ shouldConvertFixed @E3
    prop "Should convert Fixed E2" $ shouldConvertFixed @E2
    prop "Should convert Fixed E1" $ shouldConvertFixed @E1
    prop "Should convert Fixed E0" $ shouldConvertFixed @E0

    prop "Should assign Fixed E12 to correct attribute value type" $
      shouldAssignFixedToCorrectType @E12
    prop "Should assign Fixed E9 to correct attribute value type" $
      shouldAssignFixedToCorrectType @E9
    prop "Should assign Fixed E6 to correct attribute value type" $
      shouldAssignFixedToCorrectType @E6
    prop "Should assign Fixed E3 to correct attribute value type" $
      shouldAssignFixedToCorrectType @E3
    prop "Should assign Fixed E2 to correct attribute value type" $
      shouldAssignFixedToCorrectType @E2
    prop "Should convert Fixed E1 from correct attribute value type" $
      shouldConvertFixedFromCorrectType @E1
    prop "Should assign Fixed E0 to correct attribute value type" $
      shouldAssignFixedToCorrectType @E0

    prop "Should convert Fixed E12 from correct attribute value type" $
      shouldConvertFixedFromCorrectType @E12
    prop "Should convert Fixed E9 from correct attribute value type" $
      shouldConvertFixedFromCorrectType @E9
    prop "Should convert Fixed E6 from correct attribute value type" $
      shouldConvertFixedFromCorrectType @E6
    prop "Should convert Fixed E3 from correct attribute value type" $
      shouldConvertFixedFromCorrectType @E3
    prop "Should convert Fixed E2 from correct attribute value type" $
      shouldConvertFixedFromCorrectType @E2
    prop "Should assign Fixed E1 to correct attribute value type" $
      shouldAssignFixedToCorrectType @E1
    prop "Should convert Fixed E0 from correct attribute value type" $
      shouldConvertFixedFromCorrectType @E0

    it "Should convert Fixed E2 from no decimal places" $
      let av = Ddb.attributeValue & Ddb.avN ?~ "102442"
      in parseAttributeValue av `shouldBe` pure (102442.00 :: Fixed E2)

    it "Should convert Fixed E2 from 1 decimal place" $
      let av = Ddb.attributeValue & Ddb.avN ?~ "102442.3"
      in parseAttributeValue av `shouldBe` pure (102442.30 :: Fixed E2)

    it "Should convert Fixed E2 from 2 decimal places" $
      let av = Ddb.attributeValue & Ddb.avN ?~ "102442.34"
      in parseAttributeValue av `shouldBe` pure (102442.34 :: Fixed E2)

    prop "Should convert Jedi" $
      \(jedi :: Jedi) ->
        parseAttributeValue (toAttributeValue jedi) `shouldBe` pure jedi

    prop "Should assign Jedi to correct attribute value type" $
      \(jedi :: Jedi) -> do
        let expected = Ddb.attributeValue & Ddb.avS ?~ text1ToText (toText1 jedi)
        toAttributeValue jedi `shouldBe` expected

    prop "Should convert Jedi from correct attribute value type" $
      \(jedi :: Jedi) -> do
        let av = Ddb.attributeValue & Ddb.avS ?~ text1ToText (toText1 jedi)
        parseAttributeValue av `shouldBe` pure jedi

    prop "Should convert wrapped type for text newtypes" $
      \(barName :: BarName) ->
        parseAttributeValue (toAttributeValue barName) `shouldBe` pure barName

    prop "Should use wrapped type for non-null text newtypes" $
      \(barName@BarName{..} :: BarName) -> do
        let expected = Ddb.attributeValue & Ddb.avS ?~ text1ToText _barName
        toAttributeValue barName `shouldBe` expected

    prop "Should convert wrapped type for non-null text newtypes" $
      \(barName@BarName{..} :: BarName) -> do
        let av = Ddb.attributeValue & Ddb.avS ?~ text1ToText _barName
        parseAttributeValue av `shouldBe` pure barName

    prop "Should convert wrapped type for int newtypes" $
      \(barNumber :: BarNumber) ->
        parseAttributeValue (toAttributeValue barNumber) `shouldBe` pure barNumber

    prop "Should use wrapped type for int newtypes" $
      \(barNumber@BarNumber{..} :: BarNumber) -> do
        let expected = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 _barNumber)
        toAttributeValue barNumber `shouldBe` expected

    prop "Should convert wrapped type for int newtypes" $
      \(barNumber@BarNumber{..} :: BarNumber) -> do
        let av = Ddb.attributeValue & Ddb.avN ?~ text1ToText (toText1 _barNumber)
        parseAttributeValue av `shouldBe` pure barNumber

    prop "Should convert ByteString list" $
      \(byteStrings :: [ByteString]) ->
        parseAttributeValue (toAttributeValue byteStrings) `shouldBe` pure byteStrings

    prop "Should assign ByteString list to correct attribute value type" $
      \(byteStrings :: [ByteString]) -> do
        let expected = Ddb.attributeValue & Ddb.avL .~ (toAttributeValue <$> byteStrings)
        toAttributeValue byteStrings `shouldBe` expected

    prop "Should convert ByteString list from correct attribute value type" $
      \(byteStrings :: [ByteString]) -> do
        let av = Ddb.attributeValue & Ddb.avL .~ (toAttributeValue <$> byteStrings)
        parseAttributeValue av `shouldBe` pure byteStrings

    prop "Should convert Text1 list" $
      \(texts :: [Text1]) ->
        parseAttributeValue (toAttributeValue texts) `shouldBe` pure texts

    prop "Should assign Text1 list to correct attribute value type" $
      \(texts :: [Text1]) -> do
        let expected = Ddb.attributeValue & Ddb.avL .~ (toAttributeValue <$> texts)
        toAttributeValue texts `shouldBe` expected

    prop "Should convert Text1 list from correct attribute value type" $
      \(texts :: [Text1]) -> do
        let av = Ddb.attributeValue & Ddb.avL .~ (toAttributeValue <$> texts)
        parseAttributeValue av `shouldBe` pure texts

    prop "Should convert Int list" $
      \(ints :: [Int]) ->
        parseAttributeValue (toAttributeValue ints) `shouldBe` pure ints

    prop "Should assign Int list to correct attribute value type" $
      \(ints :: [Int]) -> do
        let expected = Ddb.attributeValue & Ddb.avL .~ (toAttributeValue <$> ints)
        toAttributeValue ints `shouldBe` expected

    prop "Should convert Int list from correct attribute value type" $
      \(ints :: [Int]) -> do
        let av = Ddb.attributeValue & Ddb.avL .~ (toAttributeValue <$> ints)
        parseAttributeValue av `shouldBe` pure ints

    prop "Should convert Jedi list" $
      \(jedi :: [Jedi]) ->
        parseAttributeValue (toAttributeValue jedi) `shouldBe` pure jedi

    prop "Should assign Jedi list to correct attribute value type" $
      \(jedi :: [Jedi]) -> do
        let list = (\j -> Ddb.attributeValue & Ddb.avS ?~ text1ToText (toText1 j)) <$> jedi
        let expected = Ddb.attributeValue & Ddb.avL .~ list
        toAttributeValue jedi `shouldBe` expected

    prop "Should convert Jedi list from correct attribute value type" $
      \(jedi :: [Jedi]) -> do
        let list = (\j -> Ddb.attributeValue & Ddb.avS ?~ text1ToText (toText1 j)) <$> jedi
        let av = Ddb.attributeValue & Ddb.avL .~ list
        parseAttributeValue av `shouldBe` pure jedi

    prop "Should convert Maybe if has To/FromAttributeValue instances" $
      \(bool :: Maybe Bool) ->
        parseAttributeValue (toAttributeValue bool) `shouldBe` pure bool

    prop "Should assign Just to correct attribute value type" $
      \(bool :: Bool) -> do
        let expected = Ddb.attributeValue & Ddb.avBOOL ?~ bool
        toAttributeValue (Just bool) `shouldBe` expected

    it "Should assign Nothing to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Nothing :: Maybe Bool) `shouldBe` expected

    prop "Should convert Text1 set" $
      \(texts :: Set Text1) ->
        parseAttributeValue (toAttributeValue texts) `shouldBe` pure texts

    prop "Should assign Text1 set to correct attribute value type" $
      \(texts :: Set Text1) -> texts /= Set.empty ==> do
        let expected = Ddb.attributeValue & Ddb.avSS .~ (text1ToText <$> Set.toList texts)
        toAttributeValue texts `shouldBe` expected

    it "Should assign empty Text1 set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Text1) `shouldBe` expected

    prop "Should convert Text1 set from correct attribute value type" $
      \(texts :: Set Text1) -> texts /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avSS .~ (text1ToText <$> Set.toList texts)
        parseAttributeValue av `shouldBe` pure texts

    prop "Should convert ByteString set" $
      \(byteStrings :: Set ByteString) ->
        parseAttributeValue (toAttributeValue byteStrings) `shouldBe` pure byteStrings

    prop "Should assign non-empty ByteString set to correct attribute value type" $
      \(byteStrings :: Set ByteString) -> byteStrings /= Set.empty ==> do
        let expected = Ddb.attributeValue & Ddb.avBS .~ Set.toList byteStrings
        toAttributeValue byteStrings `shouldBe` expected

    it "Should assign empty ByteString set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set ByteString) `shouldBe` expected

    prop "Should convert non-empty ByteString set from correct attribute value type" $
      \(byteStrings :: Set ByteString) -> byteStrings /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avBS .~ Set.toList byteStrings
        parseAttributeValue av `shouldBe` pure byteStrings

    it "Should convert empty ByteString set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set ByteString)

    prop "Should convert Double set" $
      \(doubles :: Set Double) ->
        parseAttributeValue (toAttributeValue doubles) `shouldBe` pure doubles

    prop "Should assign non-empty Double set to correct attribute value type" $
      \(doubles :: Set Double) -> doubles /= Set.empty ==> do
        let expected = Ddb.attributeValue & Ddb.avNS .~ (sort $ toText <$> Set.toList doubles)
        toAttributeValue doubles `shouldBe` expected

    it "Should assign empty Double set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Double) `shouldBe` expected

    prop "Should convert IntSet" $
      \(ints :: IntSet) ->
        parseAttributeValue (toAttributeValue ints) `shouldBe` pure ints

    prop "Should assign non-empty IntSet to correct attribute value type" $
      \(ints :: IntSet) -> ints /= IS.empty ==> do
        let expected = Ddb.attributeValue
              & Ddb.avNS .~ (toText <$> IS.toList ints)
        toAttributeValue ints `shouldBe` expected

    it "Should assign empty IntSet to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue IS.empty `shouldBe` expected

    prop "Should convert non-empty IntSet from correct attribute value type" $
      \(ints :: IntSet) -> ints /= IS.empty ==> do
        let av = Ddb.attributeValue & Ddb.avNS .~ (toText <$> IS.toList ints)
        parseAttributeValue av `shouldBe` pure ints

    it "Should convert empty Int set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure IS.empty

    prop "Should convert Int set" $
      \(ints :: Set Int) ->
        parseAttributeValue (toAttributeValue ints) `shouldBe` pure ints

    prop "Should assign non-empty Int set to correct attribute value type" $
      \(ints :: Set Int) -> ints /= Set.empty ==> do
        let expected = Ddb.attributeValue
              & Ddb.avNS .~ sort (toText <$> Set.toList ints)
        toAttributeValue ints `shouldBe` expected

    it "Should assign empty Int set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Int) `shouldBe` expected

    prop "Should convert non-empty Int set from correct attribute value type" $
      \(ints :: Set Int) -> ints /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avNS .~ (toText <$> Set.toList ints)
        parseAttributeValue av `shouldBe` pure ints

    it "Should convert empty Int set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set Int)

    prop "Should convert Int64 set" $
      \(ints :: Set Int64) ->
        parseAttributeValue (toAttributeValue ints) `shouldBe` pure ints

    prop "Should assign non-empty Int64 set to correct attribute value type" $
      \(ints :: Set Int64) -> ints /= Set.empty ==> do
        let expected = Ddb.attributeValue
              & Ddb.avNS .~ sort (toText <$> Set.toList ints)
        toAttributeValue ints `shouldBe` expected

    it "Should assign empty Int64 set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Int64) `shouldBe` expected

    prop "Should convert non-empty Int64 set from correct attribute value type" $
      \(ints :: Set Int64) -> ints /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avNS .~ (toText <$> Set.toList ints)
        parseAttributeValue av `shouldBe` pure ints

    it "Should convert empty Int64 set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set Int64)

    prop "Should convert Integer set" $
      \(ints :: Set Integer) ->
        parseAttributeValue (toAttributeValue ints) `shouldBe` pure ints

    prop "Should assign non-empty Integer set to correct attribute value type" $
      \(ints :: Set Integer) -> ints /= Set.empty ==> do
        let expected = Ddb.attributeValue
              & Ddb.avNS .~ sort (toText <$> Set.toList ints)
        toAttributeValue ints `shouldBe` expected

    it "Should assign empty Integer set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Integer) `shouldBe` expected

    prop "Should convert non-empty Integer set from correct attribute value type" $
      \(ints :: Set Integer) -> ints /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avNS .~ (toText <$> Set.toList ints)
        parseAttributeValue av `shouldBe` pure ints

    it "Should convert empty Integer set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set Integer)

    prop "Should convert Natural set" $
      \(naturals :: Set Natural) ->
        parseAttributeValue (toAttributeValue naturals) `shouldBe` pure naturals

    prop "Should assign non-empty Natural set to correct attribute value type" $
      \(naturals :: Set Natural) -> naturals /= Set.empty ==> do
        let expected = Ddb.attributeValue
              & Ddb.avNS .~ sort (toText <$> Set.toList naturals)
        toAttributeValue naturals `shouldBe` expected

    it "Should assign empty Natural set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Natural) `shouldBe` expected

    prop "Should convert non-empty Natural set from correct attribute value type" $
      \(naturals :: Set Natural) -> naturals /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avNS .~ (toText <$> Set.toList naturals)
        parseAttributeValue av `shouldBe` pure naturals

    it "Should convert empty Natural set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set Natural)

    prop "Should convert Nat set" $
      \(naturals :: Set Natural) -> do
        let nats = Set.map Nat naturals
        parseAttributeValue (toAttributeValue nats) `shouldBe` pure nats

    prop "Should assign non-empty Nat set to correct attribute value type" $
      \(naturals :: Set Natural) -> naturals /= Set.empty ==> do
        let nats = Set.map Nat naturals
            expected = Ddb.attributeValue
              & Ddb.avNS .~ sort (toText <$> Set.toList nats)
        toAttributeValue nats `shouldBe` expected

    it "Should assign empty Nat set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Nat) `shouldBe` expected

    prop "Should convert non-empty Nat set to correct attribute value type" $
      \(naturals :: Set Natural) -> naturals /= Set.empty ==> do
        let nats = Set.map Nat naturals
            av = Ddb.attributeValue & Ddb.avNS .~ (toText <$> Set.toList nats)
        parseAttributeValue av `shouldBe` pure nats

    it "Should convert empty Nat set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set Nat)

    prop "Should convert Scientific set" $
      \(scientific :: Set Scientific) ->
        parseAttributeValue (toAttributeValue scientific) `shouldBe` pure scientific

    prop "Should assign non-empty Scientific set to correct attribute value type" $
      \scientific (scientifics' :: Set Scientific) -> do
        let scientifics = Set.insert scientific scientifics'
            expected = Ddb.attributeValue
              & Ddb.avNS .~ sort (toText <$> Set.toList scientifics)
        toAttributeValue scientifics `shouldBe` expected

    it "Should assign empty Scientific set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Scientific) `shouldBe` expected

    prop "Should convert non-empty Scientific set from correct attribute value type" $
      \(scientifics :: Set Scientific) -> scientifics /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avNS .~ (toText <$> Set.toList scientifics)
        parseAttributeValue av `shouldBe` pure scientifics

    it "Should convert empty Scientific set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set Scientific)

    prop "Should assign wrapped type for non-empty set of text newtypes" $
      \barName (barNames' :: Set BarName) -> do
        let barNames = Set.insert barName barNames'
            expected = Ddb.attributeValue & Ddb.avSS .~ (text1ToText . _barName <$> Set.toList barNames)
        toAttributeValue barNames `shouldBe` expected

    it "Should assign wrapped type for empty set of text newtypes" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set BarName) `shouldBe` expected

    prop "Should convert wrapped type for non-empty set of text newtypes" $
      \barName (barNames' :: Set BarName) -> do
        let barNames = Set.insert barName barNames'
            av = Ddb.attributeValue & Ddb.avSS .~ (text1ToText . _barName <$> Set.toList barNames)
        parseAttributeValue av `shouldBe` pure barNames

    it "Should convert wrapped type for empty set of text newtypes" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set BarName)

    prop "Should assign wrapped type for non-empty set of int newtypes" $
      \barNumber (barNumbers' :: Set BarNumber) -> do
        let barNumbers = Set.insert barNumber barNumbers'
            numbers = sort $ toText . _barNumber <$> Set.toList barNumbers
            expected = Ddb.attributeValue & Ddb.avNS .~ numbers
        toAttributeValue barNumbers `shouldBe` expected

    it "Should assign wrapped type for empty set of int newtypes" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set BarNumber) `shouldBe` expected

    prop "Should convert wrapped type for non-empty set of int newtypes" $
      \barNumber (barNumbers' :: Set BarNumber) -> do
        let barNumbers = Set.insert barNumber barNumbers'
            numbers = Set.toList $ Set.map (toText . _barNumber) barNumbers
            av = Ddb.attributeValue & Ddb.avNS .~ numbers
        parseAttributeValue av `shouldBe` pure barNumbers

    it "Should convert wrapped type for empty set of int newtypes" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set BarNumber)

    prop "Should convert Jedi set" $
      \(jedi :: Set Jedi) ->
        parseAttributeValue (toAttributeValue jedi) `shouldBe` pure jedi

    prop "Should assign non-empty Jedi set to correct attribute value type" $
      \(jedi :: Set Jedi) -> jedi /= Set.empty ==> do
        let expected = Ddb.attributeValue & Ddb.avSS .~ Set.toList (Set.map toText jedi)
        toAttributeValue jedi `shouldBe` expected

    it "Should assign empty Jedi set to correct attribute value type" $ do
      let expected = Ddb.attributeValue & Ddb.avNULL ?~ True
      toAttributeValue (Set.empty :: Set Jedi) `shouldBe` expected

    prop "Should convert non-empty Jedi set from correct attribute value type" $
      \(jedi :: Set Jedi) -> jedi /= Set.empty ==> do
        let av = Ddb.attributeValue & Ddb.avSS .~ Set.toList (Set.map toText jedi)
        parseAttributeValue av `shouldBe` pure jedi

    it "Should convert empty Jedi set from correct attribute value type" $ do
      let av = Ddb.attributeValue & Ddb.avNULL ?~ True
      parseAttributeValue av `shouldBe` pure (Set.empty :: Set Jedi)

    prop "Should convert HashMaps Text1 ByteString" $
      \(hashMaps :: HashMap Text1 ByteString) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps Text1 ByteString to correct attribute value type" $
      \(hashMaps :: HashMap Text1 ByteString) -> do
        let expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert HashMaps Text1 ByteString from correct attribute value type" $
      \(hashMaps :: HashMap Text1 ByteString) -> do
        let av = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        parseAttributeValue av `shouldBe` pure hashMaps

    prop "Should convert HashMaps Text1 Thyme.UTCTime" $
      \(hashMaps :: HashMap Text1 Thyme.UTCTime) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps Text1 Thyme.UTCTime to correct attribute value type" $
      \(hashMaps :: HashMap Text1 Thyme.UTCTime) -> do
        let expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert HashMaps Text1 Bool" $
      \(hashMaps :: HashMap Text1 Bool) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps Text1 Bool to correct attribute value type" $
      \(hashMaps :: HashMap Text1 Bool) -> do
        let expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert HashMaps Text1 Text1" $
      \(hashMaps :: HashMap Text1 Text1) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps Text1 Text1 to correct attribute value type" $
      \(hashMaps :: HashMap Text1 Text1) -> do
        let expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert HashMaps Text1 Int" $
      \(hashMaps :: HashMap Text1 Int) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps Text Int to correct attribute value type" $
      \(hashMaps :: HashMap Text1 Int) -> do
        let expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert HashMaps Text Int64" $
      \(hashMaps :: HashMap Text1 Int64) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps Text Int64 to correct attribute value type" $
      \(hashMaps :: HashMap Text1 Int64) -> do
        let expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert HashMaps if has To/FromText1 instances" $
      \(hashMaps :: HashMap Text1 Jedi) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps if has To/FromText1 instances to correct attribute value type" $
      \(hashMaps :: HashMap Text1 Jedi) -> do
        let expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys (toAttributeValue <$> hashMaps)
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert HashMaps if key has To/FromText1 instances" $
      \(hashMaps :: HashMap Jedi Text1) ->
        parseAttributeValue (toAttributeValue hashMaps) `shouldBe` pure hashMaps

    prop "Should assign HashMaps if key has To/FromText1 instances to correct attribute value type" $
      \(hashMaps :: HashMap Jedi Text1) -> do
        let av = HashMap.fromList $ bimap toText toAttributeValue <$> HashMap.toList hashMaps
            expected = Ddb.attributeValue & Ddb.avM .~ av
        toAttributeValue hashMaps `shouldBe` expected

    prop "Should convert Maps Text1 ByteString" $
      \(maps :: Map Text1 ByteString) ->
        parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps

    prop "Should assign Maps Text1 ByteString to correct attribute value type" $
      \(maps :: Map Text1 ByteString) -> do
        let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
            expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        toAttributeValue maps `shouldBe` expected

    prop "Should convert Maps Text1 ByteString from correct attribute value type" $
      \(maps :: Map Text1 ByteString) -> do
        let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
            av = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        parseAttributeValue av `shouldBe` pure maps

    -- prop "Should convert Maps Text1 Thyme.UTCTime" $
    --   \(maps :: Map Text1 Thyme.UTCTime) ->
    --     parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps
    --
    -- prop "Should assign Maps Text1 Thyme.UTCTime to correct attribute value type" $
    --   \(maps :: Map Text1 Thyme.UTCTime) -> do
    --     let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
    --         expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
    --     toAttributeValue maps `shouldBe` expected

    prop "Should convert Maps Text1 Bool" $
      \(maps :: Map Text1 Bool) ->
        parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps

    prop "Should assign Maps Text1 Bool to correct attribute value type" $
      \(maps :: Map Text1 Bool) -> do
        let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
            expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        toAttributeValue maps `shouldBe` expected

    prop "Should convert Maps Text1 Text1" $
      \(maps :: Map Text1 Text1) ->
        parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps

    prop "Should assign Maps Text1 Text1 to correct attribute value type" $
      \(maps :: Map Text1 Text1) -> do
        let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
            expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        toAttributeValue maps `shouldBe` expected

    prop "Should convert Maps Text1 Int" $
      \(maps :: Map Text1 Int) ->
        parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps

    prop "Should assign Maps Text1 Int to correct attribute value type" $
      \(maps :: Map Text1 Int) -> do
        let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
            expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        toAttributeValue maps `shouldBe` expected

    prop "Should convert Maps Text1 Int64" $
      \(maps :: Map Text1 Int64) ->
        parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps

    prop "Should assign Maps Text1 Int64 to correct attribute value type" $
      \(maps :: Map Text1 Int64) -> do
        let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
            expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        toAttributeValue maps `shouldBe` expected

    prop "Should convert Maps if has To/FromText1 instances" $
      \(maps :: Map Text1 Jedi) ->
        parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps

    prop "Should assign Maps if has To/FromText1 instances to correct attribute value type" $
      \(maps :: Map Text1 Jedi) -> do
        let hashMap = HashMap.fromList $ Map.toList $ toAttributeValue <$> maps
            expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        toAttributeValue maps `shouldBe` expected

    prop "Should convert Maps if key has To/FromText1 instances" $
      \(maps :: Map Jedi Text1) ->
        parseAttributeValue (toAttributeValue maps) `shouldBe` pure maps

    prop "Should assign Maps if key has To/FromText1 instances to correct attribute value type" $
      \(maps :: Map Jedi Text1) -> do
        let hashMap = HashMap.fromList $ bimap toText1 toAttributeValue <$> Map.toList maps
            expected = Ddb.attributeValue & Ddb.avM .~ fromText1Keys hashMap
        toAttributeValue maps `shouldBe` expected

    prop "Should convert all of the things" $
      \(allTheThings :: [Map Text1 [Jedi]]) ->
        parseAttributeValue (toAttributeValue allTheThings) `shouldBe` pure allTheThings

    it "Should round-trip a JSON Number" $ do
      let value = Number 123
      parseAttributeValue (toAttributeValue value) `shouldBe` pure value

    it "Should round-trip a JSON String" $ do
      let value = String "hello"
      parseAttributeValue (toAttributeValue value) `shouldBe` pure value

    it "Should flatten empty JSON String to null" $ do
      let value = String ""
      parseAttributeValue (toAttributeValue value) `shouldBe` pure Null

    it "Should round-trip a JSON Null" $ do
      let value = Null
      parseAttributeValue (toAttributeValue value) `shouldBe` pure value

    it "Should round-trip a JSON Bool" $ do
      let value = Bool True
      parseAttributeValue (toAttributeValue value) `shouldBe` pure value

    it "Should round-trip an empty JSON Object" $ do
      let value = Object mempty
      parseAttributeValue (toAttributeValue value) `shouldBe` pure value

    it "Should round-trip a non-empty JSON Object" $ do
      let value = object ["key" .= String "value"]
      parseAttributeValue (toAttributeValue value) `shouldBe` pure value

  context "AttributeValues" $ do

    prop "to/fromAttributeValues round trip" $
      \(bar :: Bar) -> do
        parseEither fromAttributeValues (toAttributeValues bar) `shouldBe` pure bar
        parseEither fromAttributeValues' (toAttributeValues' bar) `shouldBe` pure bar

    prop "to/fromAttributeValue because of AttributeValues instance" $
      \(bar :: Bar) ->
        parseEither fromAttributeValue (toAttributeValue bar) `shouldBe` pure bar

    prop "Should convert HashMap if key has To/FromText1 instances" $
      \(hashmap :: HashMap Jedi Text1) -> do
        parseEither fromAttributeValues (toAttributeValues hashmap) `shouldBe` pure hashmap
        parseEither fromAttributeValues' (toAttributeValues' hashmap) `shouldBe` pure hashmap

    prop "Should convert Maps if key has To/FromText1 instances" $
      \(maps :: Map Jedi Text1) -> do
        parseEither fromAttributeValues (toAttributeValues maps) `shouldBe` pure maps
        parseEither fromAttributeValues' (toAttributeValues' maps) `shouldBe` pure maps

  context "Parser" $ do

    it "Alternative should succeed when left succeeds" $ do
      let string = toAttributeValue ("1" :: Text1)
          int = toAttributeValue (1 :: Int)
      (parseAttributeValue int <|> parseAttributeValue string) `shouldBe` pure (1 :: Int)

    it "Alternative should succeed when right succeeds" $ do
      let string = toAttributeValue ("1" :: Text1)
          int = toAttributeValue (1 :: Int)
      (parseAttributeValue string <|> parseAttributeValue int) `shouldBe` pure (1 :: Int)

    it "Alternative should fail from right when all fail" $ do
      let string1 = toAttributeValue ("1" :: Text1)
          string2 = toAttributeValue ("2" :: Text1)
      (parseAttributeValue string1 <|> parseAttributeValue string2 <|> Left "No int found")
        `shouldBe` (Left "No int found" :: Either String Int)

  context "fromDynamoList" $

    it "Should keep nulls" $
      let result = fromDynamoList
            [ "Name"          ..= Just (1 :: Int)
            , "Song"          ..= (Nothing :: Maybe Text1)
            , "VisitedBy"     ..= Just ObiWanKenobi ]
      in HashMap.keys result `shouldMatchList` ["Name", "Song", "VisitedBy"]

newtype BarName =
  BarName
    { _barName :: Text1 }
  deriving (Show, Eq, Ord, ToAttributeValue, FromAttributeValue, Arbitrary)

deriving instance Generic BarName
instance Newtype BarName

newtype BarNumber =
  BarNumber
    { _barNumber :: Int }
  deriving (Show, Eq, Ord, ToAttributeValue, FromAttributeValue, Arbitrary)

deriving instance Generic BarNumber
instance Newtype BarNumber

data Bar =
  Bar
    { _bName      :: BarName
    , _bSong      :: Text1
    , _bVisitedBy :: [Jedi] }
  deriving (Show, Eq)

instance Arbitrary Bar where
  arbitrary =
    Bar
    <$> arbitrary
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
      , "VisitedBy"     ..= _bVisitedBy ]

  fromAttributeValues e =
    Bar
      <$> e ..: "Name"
      <*> e ..: "Song"
      <*> e ..: "VisitedBy"

instance DynamoEntity Bar where
  type Key Bar = BarName
  getKey Bar{..} = _bName

data LightSaber =
  LightSaber
    { _lsColour       :: Text1
    , _lsJedi         :: Jedi
    , _lsDateAcquired :: Thyme.UTCTime }

instance AttributeValues (Jedi, Thyme.UTCTime) where
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
  type Key LightSaber = (Jedi, Thyme.UTCTime)
  getKey LightSaber{..} = (_lsJedi, _lsDateAcquired)

data Jedi =
    ObiWanKenobi
  | Luke
  | Yoda
  deriving (Show, Eq, Ord, Generic, Bounded, Enum)
  deriving (ToAttributeValue, FromAttributeValue) via AsText1 Jedi

instance Hashable Jedi

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

instance ToAttributeValue (Set Jedi) where
  toAttributeValue = toText1SetAttributeValue

instance FromAttributeValue (Set Jedi) where
  fromAttributeValue = fromText1SetAttributeValue

instance Arbitrary Jedi where
  arbitrary = arbitraryBoundedEnum
