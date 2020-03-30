{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Test.Hspec.AWS.DynamoDB
  ( DynamoConfig(..)
  , HasDynamoConfig(..)
  , defaultConfig
  , DynamoKeyElement(..)
  , KeyType(..)
  , ScalarAttributeType(..)
  , SecondaryIndex(..)
  , ProjectionType(..)
  , DynamoLocal(..)
  , TableName
  , withDynamoLocal
  ) where

import           Control.Lens (makeClassy, (&), (.~), (<&>), (?~), (^.))
import           Control.Monad (void, when)
import           Control.Monad.Trans.AWS
                  (AWST, AccessKey(..), Credentials(FromKeys), SecretKey(..), configure, newEnv,
                  paginate, runAWST, runResourceT, send, setEndpoint)
import           Control.Monad.Trans.Resource (ResourceT)
import           Data.Conduit (runConduit, (.|))
import qualified Data.Conduit.List as C
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import qualified Data.List as List
import           Data.List.NonEmpty as NE
import           Data.List.NonEmpty (NonEmpty)
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Text (Text)
import           Data.Text.Encoding (encodeUtf8)
import           Network.AWS.DynamoDB
                  (AttributeDefinition, AttributeValue, DeleteItem, GlobalSecondaryIndex,
                  KeySchemaElement, KeyType(..), ProjectionType(..), ProvisionedThroughput,
                  ScalarAttributeType(..), ScanResponse, attributeDefinition, dynamoDB,
                  globalSecondaryIndex, keySchemaElement, pNonKeyAttributes, pProjectionType,
                  projection, provisionedThroughput)
import           Network.AWS.DynamoDB.CreateTable
                  (createTable, ctAttributeDefinitions, ctGlobalSecondaryIndexes)
import           Network.AWS.DynamoDB.DeleteItem (deleteItem, diKey)
import           Network.AWS.DynamoDB.DeleteTable (deleteTable)
import           Network.AWS.DynamoDB.Scan (scan, srsItems)
import           Numeric.Natural (Natural)
import           Test.Hspec (Spec, SpecWith, after, afterAll, beforeAll, describe)

import           Network.AWS.DynamoDB.Extra (TableName, (.!~))

data DynamoConfig =
  DynamoConfig
    { _dcHostName               :: Text
    , _dcIsSecure               :: Bool
    , _dcPort                   :: Int
    , _dcTableName              :: TableName
    , _dcDynamoKeyElements      :: NonEmpty DynamoKeyElement
    , _dcGlobalSecondaryIndexes :: [SecondaryIndex]
    , _dcProvisionedReadUnits   :: Natural
    , _dcProvisionedWriteUnits  :: Natural
    , _dcResetAfterEachTest     :: Bool
    , _dcCredentials            :: Credentials }

-- It might also be possible to generate the key schema and attribute definitions
-- using our To/FromAttributeValue type classes, but this is all good enough for now.
data DynamoKeyElement =
  DynamoKeyElement
    { _dkeAttributeName :: Text
    , _dkeKeyType       :: KeyType
    , _dkeScalarType    :: ScalarAttributeType }

data SecondaryIndex =
  SecondaryIndex
    { _siIndexName        :: Text
    , _siKeySchema        :: NonEmpty DynamoKeyElement
    , _siProjectionType   :: ProjectionType
    , _siNonKeyAttributes :: [Text] }

makeClassy ''DynamoConfig

defaultConfig :: DynamoConfig
defaultConfig = DynamoConfig
  { _dcHostName               = "localhost"
  , _dcIsSecure               = False
  , _dcPort                   = 8000
  , _dcTableName              = "integration-tests"
  , _dcDynamoKeyElements      = NE.singleton $ DynamoKeyElement "Id" Hash N
  , _dcGlobalSecondaryIndexes = []
  , _dcProvisionedReadUnits   = 200
  , _dcProvisionedWriteUnits  = 200
  , _dcResetAfterEachTest     = True
  , _dcCredentials            = FromKeys (AccessKey "Dummy") (SecretKey "Dummy") }

data DynamoLocal =
  DynamoLocal
    { _dlTableName :: TableName
    , _dlRunAws    :: forall a. AWST (ResourceT IO) a -> IO a }

withDynamoLocal :: DynamoConfig -> SpecWith DynamoLocal -> Spec
withDynamoLocal DynamoConfig{..} =
  beforeAll initDynamo
  . afterAll tearDownDynamo
  . after clearDynamoTable
  . describe "Dynamo Integration Tests"
  where
    initDynamo :: IO DynamoLocal
    initDynamo = do
      env <- newEnv _dcCredentials
              <&> configure (setEndpoint _dcIsSecure (encodeUtf8 _dcHostName) _dcPort dynamoDB)
      let keySchemaElements = toKeySchemaElement <$> _dcDynamoKeyElements
          provisionedThroughput' =
            provisionedThroughput _dcProvisionedReadUnits _dcProvisionedWriteUnits
          globalSecondaryIndexes =
            toGlobalSecondaryIndex provisionedThroughput' <$> _dcGlobalSecondaryIndexes
          request = createTable _dcTableName keySchemaElements provisionedThroughput'
                      & ctAttributeDefinitions .~ attributeDefinitions
                      & ctGlobalSecondaryIndexes .!~ globalSecondaryIndexes
      runResourceT $ runAWST env $ void $ send request
      pure $ DynamoLocal _dcTableName $ runResourceT . runAWST env

    toKeySchemaElement :: DynamoKeyElement -> KeySchemaElement
    toKeySchemaElement DynamoKeyElement{..} =
      keySchemaElement _dkeAttributeName _dkeKeyType

    toAttributeDefinition :: DynamoKeyElement -> AttributeDefinition
    toAttributeDefinition DynamoKeyElement{..} =
      attributeDefinition _dkeAttributeName _dkeScalarType

    toGlobalSecondaryIndex :: ProvisionedThroughput -> SecondaryIndex -> GlobalSecondaryIndex
    toGlobalSecondaryIndex provisionedThroughput' SecondaryIndex{..} =
      let projection' = projection
           & pProjectionType ?~ _siProjectionType
           & pNonKeyAttributes .~ NE.nonEmpty _siNonKeyAttributes
      in globalSecondaryIndex
          _siIndexName
          (toKeySchemaElement <$> _siKeySchema)
          projection'
          provisionedThroughput'

    attributeDefinitions :: [AttributeDefinition]
    attributeDefinitions =
      let keyAttributeDefinitions = NE.toList $ toAttributeDefinition <$> _dcDynamoKeyElements
          indexAttributeDefinitions =
            toAttributeDefinition <$> concatMap (NE.toList . _siKeySchema) _dcGlobalSecondaryIndexes
      in List.nub $ keyAttributeDefinitions <> indexAttributeDefinitions

    tearDownDynamo :: DynamoLocal -> IO ()
    tearDownDynamo DynamoLocal{..} = _dlRunAws $ void $ send $ deleteTable _dlTableName

    clearDynamoTable :: DynamoLocal -> IO ()
    clearDynamoTable DynamoLocal{..} =
      when _dcResetAfterEachTest $ _dlRunAws $ runConduit $
        paginate (scan _dlTableName)
        .| C.concatMap getKeys
        .| C.mapM_ (void . send . toDelete)
      where
        keyNames :: Set Text
        keyNames = Set.fromList . NE.toList $ _dkeAttributeName <$> _dcDynamoKeyElements

        getKeys :: ScanResponse -> [HashMap Text AttributeValue]
        getKeys response =
          HM.filterWithKey (\key _ -> Set.member key keyNames) <$> response ^. srsItems

        toDelete :: HashMap Text AttributeValue -> DeleteItem
        toDelete key = deleteItem _dlTableName & diKey .~ key
