{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Network.AWS.DynamoDB.Extra
    ( module Network.AWS.DynamoDB.AttributeValue
    , Existing(..)
    , TableName
    , IndexName
    , DynamoEntity(..)
    , DynamoAddResult(..)
    , DynamoUpdateIfUnchangedResult(..)
    , DynamoLookupResult(..)
    , toLookupResult
    , dynamoLookupResult
    , DynamoBatchGetResponse(..)
    , DynamoBatchWriteResponse(..)
    , defaultAdd
    , defaultGet
    , defaultGetBatch
    , defaultUpdate
    , defaultUpdateIfUnchanged
    , defaultPut
    , defaultPutBatch
    , defaultDelete
    , defaultDeleteBatch
    , defaultScan
    , retryUnproccessedGets
    , retryUnproccessedWrites
    , putItemWithCondition
    , scanWithCondition
    , queryWithKeyCondition
    , queryWithKeyAndFilterCondition
    , fromText1KeyValues
    , UnparseableUnprocessedItems(..)
    , makeUnparseableUnprocessedItems
    , UnprocessedItems(..)
    , makeUnprocessedItems
    , buildUpdateExpression
    , buildExpressionAttributeNames
    , buildExpressionAttributeValues
    , buildFilteredExpressionAttributeValues
    , buildFilteredExpressionAttributeValues'
    , ConditionExpression(..)
    , Condition(..)
    , expressionToCondition
    , ContainsValue(..)
    , foldExpr
    , foldExpr'
    , AttributeType(..)
    , keyDoesNotExistCondition
    , (.!~)
    ) where

import           Control.Lens (ASetter', set', view, (&), (.~), (?~), (^.))
import           Control.Monad (void)
import           Control.Monad.Catch (Exception, MonadCatch(..), MonadThrow(..))
import           Control.Monad.Trans (MonadIO)
import           Control.Monad.Trans.AWS (AWSConstraint)
import qualified Control.Monad.Trans.AWS as Aws
import           Control.Retry (RetryPolicyM, RetryStatus, applyAndDelay, defaultRetryStatus)
import           Data.Aeson (FromJSON(..), ToJSON(..), Value(..), withText)
import           Data.Bifunctor (bimap, first)
import           Data.ByteString (ByteString)
import           Data.Conduit (ConduitT, (.|))
import qualified Data.Conduit.List as CL
import           Data.Foldable (foldl')
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import           Data.Set (Set)
import           Data.Text (Text, strip)
import           Data.Text1
                  (FromText1(..), Text1, ToText1(..), fromText1Error, fromTextM, original,
                  takeCIText1, text1ToText, toText)
import qualified Data.Text1 as Text1
import           Data.These (These(..))
import           Data.Typeable (Typeable)
import           GHC.Generics (Generic)
import           Network.AWS (trying)
import           Network.AWS.DynamoDB
                  (AttributeValue, PutItem, Query, ReturnValue(..), Scan,
                  _ConditionalCheckFailedException)
import qualified Network.AWS.DynamoDB as Ddb
import           Test.QuickCheck.Arbitrary.Generic (Arbitrary, GenericArbitrary(..))

import           Network.AWS.DynamoDB.AttributeValue

type TableName = Text
type IndexName = Text

data DynamoAddResult =
    DynamoAdded
  | DynamoAlreadyExists
  deriving (Eq, Show, Generic)
  deriving (Arbitrary) via GenericArbitrary DynamoAddResult

instance ToText1 DynamoAddResult where
  toText1 = \case
    DynamoAdded         -> "DynamoAdded"
    DynamoAlreadyExists -> "DynamoAlreadyExists"

instance FromText1 DynamoAddResult where
  parser1 = takeCIText1 >>= \case
    "dynamoadded"         -> pure DynamoAdded
    "dynamoalreadyexists" -> pure DynamoAlreadyExists
    e -> fromText1Error $ "Failure parsing DynamoAddResult from '" <> original e <> "'."

instance ToJSON DynamoAddResult where
  toJSON = String . toText

instance FromJSON DynamoAddResult where
  parseJSON = withText "DynamoAddResult" fromTextM

data DynamoUpdateIfUnchangedResult =
    DynamoUpdateSuccessful
  | DynamoUpdateDirtyRead
  deriving (Eq, Show, Generic)
  deriving (Arbitrary) via GenericArbitrary DynamoUpdateIfUnchangedResult

instance ToText1 DynamoUpdateIfUnchangedResult where
  toText1 = \case
    DynamoUpdateSuccessful -> "DynamoUpdateSuccessful"
    DynamoUpdateDirtyRead  -> "DynamoUpdateDirtyRead"

instance FromText1 DynamoUpdateIfUnchangedResult where
  parser1 = takeCIText1 >>= \case
    "dynamoupdatesuccessful" -> pure DynamoUpdateSuccessful
    "dynamoupdatedirtyread"  -> pure DynamoUpdateDirtyRead
    e -> fromText1Error $
      "Failure parsing DynamoUpdateIfUnchangedResult from '" <> original e <> "'."

instance ToJSON DynamoUpdateIfUnchangedResult where
  toJSON = String . toText

instance FromJSON DynamoUpdateIfUnchangedResult where
  parseJSON = withText "DynamoUpdateIfUnchangedResult" fromTextM

data DynamoLookupResult a =
    NotFound
  | ParseError String
  | Found a
  deriving (Show, Eq, Functor, Foldable, Traversable)

dynamoLookupResult :: b -> (String -> b) -> (a -> b) -> DynamoLookupResult a -> b
dynamoLookupResult notFound parseError found = \case
  NotFound       -> notFound
  ParseError str -> parseError str
  Found a        -> found a

data DynamoBatchGetResponse a =
    BatchGetAllProcessed [Either String a]
  | BatchGetUnprocessed [Either String a] (NonEmpty (Either String (Key a)))

deriving instance (Eq a, Eq (Key a)) => Eq (DynamoBatchGetResponse a)
deriving instance (Show a, Show (Key a)) => Show (DynamoBatchGetResponse a)

instance Semigroup (DynamoBatchGetResponse a) where
  response1 <> response2 = case (response1, response2) of
    (BatchGetAllProcessed processed1, BatchGetAllProcessed processed2) ->
      BatchGetAllProcessed $ processed1 <> processed2
    (BatchGetAllProcessed processed1, BatchGetUnprocessed processed2 unprocessed) ->
      BatchGetUnprocessed (processed1 <> processed2) unprocessed
    (BatchGetUnprocessed processed1 unprocessed, BatchGetAllProcessed processed2) ->
      BatchGetUnprocessed (processed1 <> processed2) unprocessed
    (BatchGetUnprocessed processed1 unprocessed1, BatchGetUnprocessed processed2 unprocessed2) ->
      BatchGetUnprocessed (processed1 <> processed2) (unprocessed1 <> unprocessed2)

instance Monoid (DynamoBatchGetResponse a) where
  mempty = BatchGetAllProcessed mempty
  mappend = (<>)

data DynamoBatchWriteResponse a =
    BatchWriteAllProcessed
  | BatchWriteUnprocessed (NonEmpty (Either String a))

deriving instance Eq a => Eq (DynamoBatchWriteResponse a)
deriving instance Show a => Show (DynamoBatchWriteResponse a)

instance Semigroup (DynamoBatchWriteResponse a) where
  response1 <> response2 = case (response1, response2) of
    (BatchWriteAllProcessed, response) -> response
    (response, BatchWriteAllProcessed) -> response
    (BatchWriteUnprocessed unprocessed1, BatchWriteUnprocessed unprocessed2) ->
      BatchWriteUnprocessed (unprocessed1 <> unprocessed2)

instance Monoid (DynamoBatchWriteResponse a) where
  mempty = BatchWriteAllProcessed
  mappend = (<>)

class (AttributeValues a, AttributeValues (Key a)) => DynamoEntity a where
  type Key a        :: *
  type Properties a :: *
  type Properties a = ()

  getKey :: a -> Key a

  onSave :: Properties a -> a -> a
  onSave = const id

defaultGet :: (AWSConstraint r m, DynamoEntity a, MonadCatch m) => TableName -> Key a -> m (DynamoLookupResult a)
defaultGet tableName key = do
  let request = Ddb.getItem tableName & Ddb.giKey .~ toAttributeValues' key
  toLookupResult . view Ddb.girsItem <$> Aws.send request

defaultGetBatch :: forall a r m.
  ( AWSConstraint r m
  , DynamoEntity a)
  => TableName
  -> NonEmpty (Key a)
  -> m (DynamoBatchGetResponse a)
defaultGetBatch tableName = NE.batchesOfM 100 getBatch
  where
    getBatch keys =
      let request = Ddb.batchGetItem
            & Ddb.bgiRequestItems .~ HashMap.singleton tableName (toKeysAndAttributes keys)
      in toBatchResponse <$> Aws.send request
    toKeysAndAttributes = Ddb.keysAndAttributes . fmap toAttributeValues'
    toBatchResponse response = do
      let
        unprocessedKeysAndAttributesByTable = response ^. Ddb.bgirsUnprocessedKeys
        unprocessedKeysAndAttributes = HashMap.elems unprocessedKeysAndAttributesByTable
        unprocessedKeyAttributeValues = concatMap (NE.toList . view Ddb.kaaKeys) unprocessedKeysAndAttributes
        unprocessedKeys = parseEither fromAttributeValues' <$> unprocessedKeyAttributeValues

        processedByTable = response ^. Ddb.bgirsResponses
        processedAttributeValues = concat $ HashMap.elems processedByTable
        processed = parseEither fromAttributeValues' <$> processedAttributeValues
      case unprocessedKeys of
        []  -> BatchGetAllProcessed processed
        h:t -> BatchGetUnprocessed processed $ h :| t

defaultAdd :: (AWSConstraint r m, DynamoEntity a, MonadCatch m) => TableName -> a -> m DynamoAddResult
defaultAdd tableName a = do
  let entity = toAttributeValues' a
      putItem = maybe Ddb.putItem putItemWithCondition $ keyDoesNotExistCondition a
      request = putItem tableName
        & Ddb.piItem .~ entity
  either asError asSuccess <$> trying _ConditionalCheckFailedException (Aws.send request)
  where
    asSuccess = const DynamoAdded
    asError = const DynamoAlreadyExists

defaultUpdate :: (AWSConstraint r m, DynamoEntity a) => TableName -> [Text1] -> a -> m ()
defaultUpdate tableName onlyUpdateIfNotExistKeys a = do
  let dynamoKey = toAttributeValues' $ getKey a
      entityWithoutKey = toDynamoWithoutKey a
      attributeNames = fromText1KeyValues $ buildExpressionAttributeNames entityWithoutKey
      attributeValues = fromText1Keys $ buildExpressionAttributeValues entityWithoutKey
      request = Ddb.updateItem tableName
        & Ddb.uiKey .~ dynamoKey
        & Ddb.uiUpdateExpression ?~ buildUpdateExpression onlyUpdateIfNotExistKeys entityWithoutKey
        & Ddb.uiExpressionAttributeNames .~ attributeNames
        & Ddb.uiExpressionAttributeValues .~ attributeValues
  void $ Aws.send request

newtype Existing a = Existing { unExisting :: a } deriving (Eq, Show, Ord)

defaultUpdateIfUnchanged ::
  ( AWSConstraint r m
  , DynamoEntity a
  , MonadCatch m)
  => TableName
  -> Existing a
  -> a
  -> m DynamoUpdateIfUnchangedResult
defaultUpdateIfUnchanged tableName (Existing old) new = do
  let
    oldEntity = toDynamoWithoutKey old
    newEntity = toAttributeValues' new
    condition = foldExpr And $ uncurry Eq <$> HashMap.toList oldEntity
    putItem = maybe Ddb.putItem putItemWithCondition condition
    request = putItem tableName
      & Ddb.piItem .~ newEntity
  either asError asSuccess <$> trying _ConditionalCheckFailedException (Aws.send request)
  where
    asError = const DynamoUpdateDirtyRead
    asSuccess = const DynamoUpdateSuccessful

defaultPut :: (AWSConstraint r m, DynamoEntity a) => TableName -> a -> m ()
defaultPut tableName a = void $ Aws.send $ Ddb.putItem tableName & Ddb.piItem .~ toAttributeValues' a

defaultPutBatch :: forall a r m.
  ( AWSConstraint r m
  , DynamoEntity a)
  => TableName
  -> NonEmpty a
  -> m (DynamoBatchWriteResponse a)
defaultPutBatch tableName = NE.batchesOfM 25 putBatch
  where
    putBatch items =
      let request = Ddb.batchWriteItem
            & Ddb.bwiRequestItems .~ HashMap.singleton tableName (toWriteRequest <$> items)
      in toBatchResponse <$> Aws.send request
    toWriteRequest item = Ddb.writeRequest & Ddb.wrPutRequest ?~ toPutRequest item
    toPutRequest item = Ddb.putRequest & Ddb.prItem .~ toAttributeValues' item
    toBatchResponse response = do
      let
        writeRequests = concatMap NE.toList $ HashMap.elems $ response ^. Ddb.bwirsUnprocessedItems
        putRequests = mapMaybe (view Ddb.wrPutRequest) writeRequests
      case parseEither fromAttributeValues' . view Ddb.prItem <$> putRequests of
        []  -> BatchWriteAllProcessed
        h:t -> BatchWriteUnprocessed $ h :| t

defaultDelete :: (AWSConstraint r m, DynamoEntity a) => TableName -> Key a -> m (DynamoLookupResult a)
defaultDelete tableName key = do
  let request = Ddb.deleteItem tableName
        & Ddb.diKey .~ toAttributeValues' key
        & Ddb.diReturnValues ?~ AllOld
  toLookupResult . view Ddb.dirsAttributes <$> Aws.send request

defaultDeleteBatch :: forall a r m.
  ( AWSConstraint r m
  , DynamoEntity a)
  => TableName
  -> NonEmpty (Key a)
  -> m (DynamoBatchWriteResponse (Key a))
defaultDeleteBatch tableName = NE.batchesOfM 25 deleteBatch
  where
    deleteBatch keys =
      let request = Ddb.batchWriteItem
            & Ddb.bwiRequestItems .~ HashMap.singleton tableName (toWriteRequest <$> keys)
      in toBatchResponse <$> Aws.send request
    toWriteRequest key = Ddb.writeRequest & Ddb.wrDeleteRequest ?~ toDeleteRequest key
    toDeleteRequest key = Ddb.deleteRequest & Ddb.drKey .~ toAttributeValues' key
    toBatchResponse response = do
      let
        writeRequests = concatMap NE.toList $ HashMap.elems $ response ^. Ddb.bwirsUnprocessedItems
        deleteRequests = mapMaybe (view Ddb.wrDeleteRequest) writeRequests
      case parseEither fromAttributeValues' . view Ddb.drKey <$> deleteRequests of
        []  -> BatchWriteAllProcessed
        h:t -> BatchWriteUnprocessed $ h :| t

defaultScan :: (AWSConstraint r m, DynamoEntity a) => TableName -> Maybe IndexName -> ConduitT () a m ()
defaultScan tableName indexName =
    Aws.paginate (Ddb.scan tableName & Ddb.sIndexName .~ indexName)
    .| CL.concatMap getItems
  where
    getItems :: DynamoEntity a => Ddb.ScanResponse -> [a]
    getItems response = catMaybes $ parseMaybe fromAttributeValues' <$> response ^. Ddb.srsItems

toLookupResult :: AttributeValues a => HashMap Text AttributeValue -> DynamoLookupResult a
toLookupResult item
  | null item = NotFound
  | otherwise = either ParseError Found $ parseEither fromAttributeValues' item

-- | This function can be used with batch get function to retry the unprocessed keys using the
-- provided retry policy. According to amazon, this should be safe to retry without limit as
-- if all items cannot be processed an exception is thrown.
retryUnproccessedGets :: forall a m.
  ( MonadThrow m
  , MonadIO m
  , DynamoEntity a
  , Show (Key a))
  => RetryPolicyM m
  -> (NonEmpty (Key a) -> m (DynamoBatchGetResponse a))
  -> NonEmpty (Key a)
  -> m [Either String a]
retryUnproccessedGets retryPolicy f allKeys = loop defaultRetryStatus allKeys
  where
    loop :: RetryStatus -> NonEmpty (Key a) -> m [Either String a]
    loop retryStatus keys = f keys >>= \case
      BatchGetAllProcessed results -> pure results
      BatchGetUnprocessed results unprocessed ->
        retryUnproccessed allKeys keys retryPolicy retryStatus unprocessed $
          \newRetryStatus unproccessed' -> mappend results <$> loop newRetryStatus unproccessed'

-- | This function can be used with batch write function to retry the unprocessed request using the
-- provided retry policy. According to amazon, this should be safe to retry without limit as
-- if all items cannot be processed an exception is thrown.
retryUnproccessedWrites :: forall a m.
  ( MonadThrow m
  , MonadIO m
  , Show a)
  => RetryPolicyM m
  -> (NonEmpty a -> m (DynamoBatchWriteResponse a))
  -> NonEmpty a
  -> m ()
retryUnproccessedWrites retryPolicy f allRequests = loop defaultRetryStatus allRequests
  where
    loop :: RetryStatus -> NonEmpty a -> m ()
    loop retryStatus requests = f requests >>= \case
      BatchWriteAllProcessed -> pure ()
      BatchWriteUnprocessed unprocessed ->
        retryUnproccessed allRequests requests retryPolicy retryStatus unprocessed loop

retryUnproccessed ::
  ( MonadThrow m
  , MonadIO m
  , Show a)
  => NonEmpty a
  -> NonEmpty a
  -> RetryPolicyM m
  -> RetryStatus
  -> NonEmpty (Either String a)
  -> (RetryStatus -> NonEmpty a -> m b)
  -> m b
retryUnproccessed allRequests requests retryPolicy retryStatus unprocessed f =
  case NE.partitionEithers unprocessed of
    This parseErrors ->
      throwM $ makeUnparseableUnprocessedItems allRequests parseErrors []
    These parseErrors unprocessedRequests ->
      throwM $ makeUnparseableUnprocessedItems allRequests parseErrors $ NE.toList unprocessedRequests
    That unprocessedRequests
      -- This case should never happen as AWS throw an exception if no items are processed
      | NE.length unprocessedRequests == NE.length requests ->
        throwM $ makeUnprocessedItems allRequests unprocessedRequests
      | otherwise -> applyAndDelay retryPolicy retryStatus >>= \case
        Just newRetryStatus -> f newRetryStatus unprocessedRequests
        Nothing -> throwM $ makeUnprocessedItems allRequests unprocessedRequests

data UnparseableUnprocessedItems =
  UnparseableUnprocessedItems
    { _uuiAttemptedRequests      :: NonEmpty String
    , _uuiUnprocessedParseErrors :: NonEmpty String
    , _uuiUnprocessedRequests    :: [String] }
  deriving (Eq, Show, Typeable)

instance Exception UnparseableUnprocessedItems

makeUnparseableUnprocessedItems ::
  Show a => NonEmpty a -> NonEmpty String -> [a] -> UnparseableUnprocessedItems
makeUnparseableUnprocessedItems requests unprocessedParseErrors =
  UnparseableUnprocessedItems (show <$> requests) unprocessedParseErrors . fmap show

data UnprocessedItems =
  UnprocessedItems
    { _uiAttemptedRequests   :: NonEmpty String
    , _uiUnprocessedRequests :: NonEmpty String }
  deriving (Eq, Show, Typeable)

instance Exception UnprocessedItems

makeUnprocessedItems :: Show a => NonEmpty a -> NonEmpty a -> UnprocessedItems
makeUnprocessedItems requests unprocessedRequests =
  UnprocessedItems (show <$> requests) $ show <$> unprocessedRequests

toDynamoWithoutKey :: DynamoEntity a => a -> DynamoObject
toDynamoWithoutKey a =
  let dynamoKey = toAttributeValues $ getKey a
      entity = toAttributeValues a
  in entity `HashMap.difference` dynamoKey

fromText1KeyValues :: HashMap Text1 Text1 -> HashMap Text Text
fromText1KeyValues = HashMap.fromList . fmap (bimap text1ToText text1ToText) . HashMap.toList

buildExpressionAttributeNames :: DynamoObject -> HashMap Text1 Text1
buildExpressionAttributeNames = buildExpressionAttributeNames' . HashMap.keys

buildExpressionAttributeNames' :: [Text1] -> HashMap Text1 Text1
buildExpressionAttributeNames' = HashMap.fromList . fmap (first toKey . dupe)
  where dupe a = (a, a)

buildExpressionAttributeValues :: DynamoObject -> DynamoObject
buildExpressionAttributeValues =
  HashMap.fromList
  . fmap (first toValue)
  . HashMap.toList
  . filterNullAttributeValues

buildFilteredExpressionAttributeValues :: [Text1] -> DynamoObject -> DynamoObject
buildFilteredExpressionAttributeValues keys =
    buildExpressionAttributeValues . HashMap.filterWithKey filterKeys
  where filterKeys key _ = key `elem` keys

buildFilteredExpressionAttributeValues' :: DynamoEntity a => [Text1] -> a -> DynamoObject
buildFilteredExpressionAttributeValues' keys =
  buildFilteredExpressionAttributeValues keys . toAttributeValues

buildUpdateExpression :: [Text1] -> DynamoObject -> Text
buildUpdateExpression onlyUpdateIfNotExistKeys propsMap =
  strip $ setExpr <> " " <> removeExpr
  where
    props = HashMap.toList propsMap
    setExpr = case NE.nonEmpty $ mapMaybe makeSetCmd props of
      Nothing         -> ""
      Just neSetProps -> text1ToText $ "SET " <> Text1.intercalate ", " neSetProps
    removeExpr = case NE.nonEmpty $ mapMaybe makeRemoveCmd props of
      Nothing            -> ""
      Just neRemoveProps -> text1ToText $ "REMOVE " <> Text1.intercalate ", " neRemoveProps

    makeSetCmd (key, attributeValue)
      | True <- isAttributeValueNull attributeValue = Nothing
      | key `elem` onlyUpdateIfNotExistKeys = Just buildIfNotExists
      | otherwise = Just (toKey key <> " = " <> toValue key)
      where buildIfNotExists =
              toKey key <> " = if_not_exists(" <> toKey key <> ", " <> toValue key <> ")"

    makeRemoveCmd (key, attributeValue)
      | True <- isAttributeValueNull attributeValue = Just $ toKey key
      | otherwise = Nothing

data ConditionExpression =
    forall a. ToAttributeValue a => Eq Text1 a
  | forall a. ToAttributeValue a => Ne Text1 a
  | forall a. ToAttributeValue a => Lt Text1 a
  | forall a. ToAttributeValue a => Le Text1 a
  | forall a. ToAttributeValue a => Gt Text1 a
  | forall a. ToAttributeValue a => Ge Text1 a
  | forall a. ToAttributeValue a => Between Text1 a a
  | forall a. ToAttributeValue a => In Text1 (NonEmpty a)
  | AttributeExists Text1
  | AttributeNotExists Text1
  | AttributeType Text1 AttributeType
  | BeginsWith Text1 Text1
  | Contains Text1 ContainsValue
  | Size Text1
  | And ConditionExpression ConditionExpression
  | Or ConditionExpression ConditionExpression
  | Not ConditionExpression
  | Parens ConditionExpression

data ContainsValue =
    SContains Text1
  | SsContains Text1
  | forall a. (Num a, ToAttributeValue (Set a), ToAttributeValue a) => NsContains a
  | BsContains ByteString

instance ToAttributeValue ContainsValue where
  toAttributeValue = \case
    SContains subStr -> toAttributeValue subStr
    SsContains s     -> toAttributeValue s
    NsContains n     -> toAttributeValue n
    BsContains b     -> toAttributeValue b

instance ToText1 ConditionExpression where
  toText1 = \case
    Eq key _               -> toKey key <> " = "  <> toValue key
    Ne key _               -> toKey key <> " <> " <> toValue key
    Lt key _               -> toKey key <> " < "  <> toValue key
    Le key _               -> toKey key <> " <= " <> toValue key
    Gt key _               -> toKey key <> " > "  <> toValue key
    Ge key _               -> toKey key <> " >= " <> toValue key
    Between key _ _        -> toKey key <> " BETWEEN " <> toValue' key "Min" <> " AND " <> toValue' key "Max"
    In key values          -> toKey key <> " IN ( " <> toKeys key values <> " )"
    AttributeExists key    -> "attribute_exists(" <> toKey key <> ")"
    AttributeNotExists key -> "attribute_not_exists(" <> toKey key <> ")"
    AttributeType key _    -> "attribute_type(" <> toKey key <> ", " <> toValue' key "Type" <> ")"
    BeginsWith key _       -> "begins_with(" <> toKey key <> ", " <> toValue' key "Begins" <> ")"
    Contains key _         -> "contains(" <> toKey key <> ", " <> toValue' key "Contains" <> ")"
    Size key               -> "size(" <> toKey key <> ")"
    And expr1 expr2        -> toText1 expr1 <> " AND " <> toText1 expr2
    Or expr1 expr2         -> toText1 expr1 <> " OR " <> toText1 expr2
    Not expr               -> "NOT " <> toText1 expr
    Parens expr            -> "( " <> toText1 expr <> " )"
    where
      toKeys key values =
        Text1.intercalate ", " $
          NE.zipWith (\_ n -> toValue' key $ toText1 n) values ((1 :: Int) :| [2..])

data AttributeType =
    DdbString
  | DdbStringSet
  | DdbNumber
  | DdbNumberSet
  | DdbBinary
  | DdbBinarySet
  | DdbBoolean
  | DdbNull
  | DdbList
  | DdbMap
  deriving (Show, Eq, Bounded, Enum)

instance ToText1 AttributeType where
  toText1 = \case
    DdbString    -> "S"
    DdbStringSet -> "SS"
    DdbNumber    -> "N"
    DdbNumberSet -> "NS"
    DdbBinary    -> "B"
    DdbBinarySet -> "BS"
    DdbBoolean   -> "BOOL"
    DdbNull      -> "NULL"
    DdbList      -> "L"
    DdbMap       -> "M"

instance ToAttributeValue AttributeType where
  toAttributeValue = toText1AttributeValue

data Condition =
  Condition
    { _cExpression                :: Text1
    , _cExpressionAttributeNames  :: HashMap Text1 Text1
    , _cExpressionAttributeValues :: DynamoObject }
  deriving (Eq, Show)

expressionToCondition :: ConditionExpression -> Condition
expressionToCondition expression =
  Condition
    { _cExpression                = toText1 expression
    , _cExpressionAttributeNames  = conditionExpressionAttributeNames expression
    , _cExpressionAttributeValues = conditionExpressionAttributeValues expression }

putItemWithCondition :: ConditionExpression -> TableName -> PutItem
putItemWithCondition conditionExpression tableName =
  let Condition{..} = expressionToCondition conditionExpression
  in Ddb.putItem tableName
    & Ddb.piExpressionAttributeValues .!~ fromText1Keys _cExpressionAttributeValues
    & Ddb.piExpressionAttributeNames .!~ fromText1KeyValues _cExpressionAttributeNames
    & Ddb.piConditionExpression ?~ text1ToText _cExpression

scanWithCondition :: ConditionExpression -> TableName -> Scan
scanWithCondition conditionExpression tableName =
  let Condition{..} = expressionToCondition conditionExpression
  in Ddb.scan tableName
    & Ddb.sExpressionAttributeValues .!~ fromText1Keys _cExpressionAttributeValues
    & Ddb.sExpressionAttributeNames .!~ fromText1KeyValues _cExpressionAttributeNames
    & Ddb.sFilterExpression ?~ text1ToText _cExpression

queryWithKeyCondition :: ConditionExpression -> TableName -> Query
queryWithKeyCondition keyConditionExpression tableName =
  let Condition{..} = expressionToCondition keyConditionExpression
  in Ddb.query tableName
    & Ddb.qExpressionAttributeValues .~ fromText1Keys _cExpressionAttributeValues
    & Ddb.qExpressionAttributeNames .~ fromText1KeyValues _cExpressionAttributeNames
    & Ddb.qKeyConditionExpression ?~ text1ToText _cExpression

queryWithKeyAndFilterCondition :: ConditionExpression -> ConditionExpression -> TableName -> Query
queryWithKeyAndFilterCondition keyConditionExpression filterConditionExpression tableName =
  let keyCondition = expressionToCondition keyConditionExpression
      filterCondition = expressionToCondition filterConditionExpression
      expressionAttributeValues = fromText1Keys $
        _cExpressionAttributeValues keyCondition <> _cExpressionAttributeValues filterCondition
      expressionAttributeNames = fromText1KeyValues $
        _cExpressionAttributeNames keyCondition <> _cExpressionAttributeNames filterCondition
  in Ddb.query tableName
    & Ddb.qExpressionAttributeValues .~ expressionAttributeValues
    & Ddb.qExpressionAttributeNames .~ expressionAttributeNames
    & Ddb.qKeyConditionExpression ?~ text1ToText (_cExpression keyCondition)
    & Ddb.qFilterExpression ?~ text1ToText (_cExpression filterCondition)

-- | This is the same as (.~) but only sets the value if the Folable is not null.
(.!~) :: Foldable f => ASetter' s (f a) -> f a -> s -> s
l .!~ b | null b = id
        | otherwise = set' l b

conditionExpressionAttributeNames :: ConditionExpression -> HashMap Text1 Text1
conditionExpressionAttributeNames = buildExpressionAttributeNames' . getNames
  where
    getNames = \case
      Eq key _               -> [ key ]
      Ne key _               -> [ key ]
      Lt key _               -> [ key ]
      Le key _               -> [ key ]
      Gt key _               -> [ key ]
      Ge key _               -> [ key ]
      Between key _ _        -> [ key ]
      In key _               -> [ key ]
      AttributeExists key    -> [ key ]
      AttributeNotExists key -> [ key ]
      AttributeType key _    -> [ key ]
      BeginsWith key _       -> [ key ]
      Contains key _         -> [ key ]
      Size key               -> [ key ]
      And expr1 expr2        -> getNames expr1 <> getNames expr2
      Or expr1 expr2         -> getNames expr1 <> getNames expr2
      Not expr               -> getNames expr
      Parens expr            -> getNames expr

conditionExpressionAttributeValues :: ConditionExpression -> DynamoObject
conditionExpressionAttributeValues = fromDynamoList . getValues
  where
    getValues = \case
      Eq key value            -> [ toValue key ..= value ]
      Ne key value            -> [ toValue key ..= value ]
      Lt key value            -> [ toValue key ..= value ]
      Le key value            -> [ toValue key ..= value ]
      Gt key value            -> [ toValue key ..= value ]
      Ge key value            -> [ toValue key ..= value ]
      Between key vMin vMax   -> [ toValue' key "Min" ..= vMin, toValue' key "Max" ..= vMax ]
      In key vs               -> zipWith (\v n -> toNumberedValue key n ..= v) (NE.toList vs) [1..]
      AttributeExists _       -> mempty
      AttributeNotExists _    -> mempty
      AttributeType key value -> [ toValue' key "Type" ..= value ]
      BeginsWith key value    -> [ toValue' key "Begins" ..= value ]
      Contains key value      -> [ toValue' key "Contains" ..= value ]
      Size _                  -> mempty
      And expr1 expr2         -> getValues expr1 <> getValues expr2
      Or expr1 expr2          -> getValues expr1 <> getValues expr2
      Not expr                -> getValues expr
      Parens expr             -> getValues expr
    toNumberedValue key n = toValue' key $ toText1 (n :: Int)

keyDoesNotExistCondition :: DynamoEntity a => a -> Maybe ConditionExpression
keyDoesNotExistCondition =
  foldExpr And . fmap AttributeNotExists . HashMap.keys . toAttributeValues . getKey

foldExpr ::
  (ConditionExpression -> ConditionExpression -> ConditionExpression)
  -> [ConditionExpression]
  -> Maybe ConditionExpression
foldExpr f = fmap (foldExpr' f) . NE.nonEmpty

foldExpr' ::
  (ConditionExpression -> ConditionExpression -> ConditionExpression)
  -> NonEmpty ConditionExpression
  -> ConditionExpression
foldExpr' f (h:|t) = foldl' f h t

toKey :: Text1 -> Text1
toKey key = Text1.cons '#' $ fromMaybe key $ Text1.map (\case '.' -> '_'; char -> char) key

toValue :: Text1 -> Text1
toValue key = Text1.cons ':' key

toValue' :: Text1 -> Text1 -> Text1
toValue' key suffix = toValue key <> "_" <> suffix
