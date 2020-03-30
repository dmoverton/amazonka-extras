{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}

module Control.Monad.AWS.Sqs
    ( MonadSqs(..)
    , withSqsMessages
    , SqsContext(..)
    , SqsMessage(..)
    , smReceiptHandle
    , smBody
    , ReceiptHandle(..)
    , SqsResponse(..)
    , SqsFailure(..)
    , SqsError(..)
    , SqsInvalidMessage(..)
    , HasSqsContext(..)
    , retryBatchFailures
    , batchRequests
    ) where

import           Control.DeepSeq (NFData)
import           Control.Error.Util (note)
import           Control.Lens (Lens', each, view, (%~), (&), (.~), (?~), (^.), (^..))
import           Control.Lens.TH (makeLenses)
import           Control.Monad (void)
import           Control.Monad.Catch (MonadCatch)
import           Control.Monad.Trans (MonadIO(..), MonadTrans(..))
import           Control.Monad.Trans.AWS (AWSConstraint, AWST', HasEnv)
import qualified Control.Monad.Trans.AWS as Aws
import           Control.Monad.Trans.Resource (MonadResource(..))
import           Control.Retry
                  (RetryPolicyM, RetryStatus, applyAndDelay, defaultRetryStatus,
                  exponentialBackoff, limitRetries)
import           Data.Aeson (FromJSON, ToJSON(..), eitherDecode, encode, genericToJSON)
import           Data.Aeson.Casing (aesonPrefix, camelCase)
import           Data.Bifunctor (first)
import           Data.Bitraversable (bimapM)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.Maybe (fromMaybe)
import           Data.Text (Text)
import qualified Data.Text as Text
import           Data.Text.Encoding (decodeUtf8, encodeUtf8)
import           Data.Text1 (toText)
import           Data.These (These(..))
import           GHC.Generics (Generic)
import           Network.Address.URI (URI)
import           Network.AWS.SQS
                  (deleteMessageBatch, dmbEntries, dmbrsFailed, dmbrsSuccessful, receiveMessage,
                  rmMaxNumberOfMessages, rmWaitTimeSeconds, rmrsMessages, sendMessageBatch,
                  smbEntries, smbrsFailed, smbrsSuccessful)
import qualified Network.AWS.SQS as Sqs
import           Network.AWS.SQS.Types hiding (message)
import           Test.QuickCheck.Arbitrary.Generic (Arbitrary, GenericArbitrary(..))

class Monad m => MonadSqs m where
  sqsSendMessage     :: ToJSON a => URI -> a -> m ()
  sqsSendMessages    :: (Eq a, ToJSON a) => URI -> NonEmpty a -> m (NonEmpty (a, SqsResponse))
  sqsReceiveMessages :: FromJSON a => URI -> m [Either SqsInvalidMessage (SqsMessage a)]
  sqsDeleteMessage   :: URI -> ReceiptHandle -> m ()
  sqsDeleteMessages  :: URI -> NonEmpty ReceiptHandle -> m (NonEmpty (ReceiptHandle, SqsResponse))

newtype ReceiptHandle =
  ReceiptHandle
    { _receiptHandle :: Text }
  deriving (Show, Eq, Generic)
  deriving newtype (ToJSON, NFData, Arbitrary)

type SqsId = Text

data SqsContext =
  SqsContext
    { _scMaxReceiveMessages  :: Maybe Int
    , _scPollWaitTimeSeconds :: Maybe Int }
  deriving (Show, Eq, Generic)
  deriving (Arbitrary) via GenericArbitrary SqsContext

data SqsMessage a =
  SqsMessage
    { _smReceiptHandle :: ReceiptHandle
    , _smBody          :: a }
  deriving (Eq, Generic)

data SqsResponse =
    SqsSuccess
  | SqsFailure SqsFailure
  deriving (Show, Eq, Generic)
  deriving (Arbitrary) via GenericArbitrary SqsResponse

data SqsFailure =
    SqsErrorEntry SqsError
  | SqsUnknownId
  | SqsNoResponse
  deriving (Show, Eq, Generic)
  deriving (Arbitrary) via GenericArbitrary SqsFailure

data SqsError =
  SqsError
    { _seMessage      :: Maybe Text
    , _seSendersFault :: Bool
    , _seErrorCode    :: Text }
  deriving (Show, Eq, Generic)
  deriving (Arbitrary) via GenericArbitrary SqsError

data SqsInvalidMessage =
  SqsInvalidMessage
    { _simReason        :: Text
    , _simBody          :: Maybe Text
    , _simReceiptHandle :: Maybe Text
    , _simMessageId     :: Maybe Text }
  deriving (Show, Eq, Generic)
  deriving (Arbitrary) via GenericArbitrary SqsInvalidMessage

makeLenses ''SqsMessage

class HasSqsContext a where
  sqsContext :: Lens' a SqsContext

instance HasSqsContext SqsContext where
  sqsContext = id

instance ToJSON SqsError where
  toJSON = genericToJSON $ aesonPrefix camelCase

instance {-# OVERLAPPABLE #-} (MonadTrans t, Monad (t m), MonadSqs m) => MonadSqs (t m) where
  sqsSendMessage     uri = lift . sqsSendMessage uri
  sqsSendMessages    uri = lift . sqsSendMessages uri
  sqsReceiveMessages uri = lift $ sqsReceiveMessages uri
  sqsDeleteMessage   uri = lift . sqsDeleteMessage uri
  sqsDeleteMessages  uri = lift . sqsDeleteMessages uri

instance {-# OVERLAPPING #-}
  (MonadCatch m, MonadResource m, HasEnv r, HasSqsContext r)
  => MonadSqs (AWST' r m) where
    sqsSendMessage         = sendMessage
    sqsSendMessages uri    = retryBatchFailures batchRetryPolicy $ sendMessages uri
    sqsReceiveMessages uri = view sqsContext >>= \ctx -> receiveMessages ctx uri
    sqsDeleteMessage       = deleteMessage
    sqsDeleteMessages uri  = retryBatchFailures batchRetryPolicy $ deleteMessages uri

batchRetryPolicy :: Monad m => RetryPolicyM m
batchRetryPolicy = exponentialBackoff 100000 <> limitRetries 5

-- | Allows you to process messages and delete in batches without having to handle all the states
--   manually.
withSqsMessages :: forall a m b.
  MonadSqs m =>
  (a -> m b)
  -> (b -> Bool)
  -> (SqsInvalidMessage -> b)
  -> (SqsFailure -> b -> b)
  -> URI
  -> NonEmpty (Either SqsInvalidMessage (SqsMessage a))
  -> m (NonEmpty b)
withSqsMessages
  processMessage
  shouldDelete
  fromInvalidMessage
  fromDeleteFailure
  queueUri
  messages = case NE.partitionEithers messages of
    This  invalidMessages -> pure $ fromInvalidMessage <$> invalidMessages
    That  validMessages   -> processValidMessages validMessages
    These invalidMessages validMessages -> do
      let invalidMessageResults = fromInvalidMessage <$> invalidMessages
      results <- processValidMessages validMessages
      pure $ results <> invalidMessageResults
  where
    processValidMessages :: NonEmpty (SqsMessage a) -> m (NonEmpty b)
    processValidMessages validMessages = do
      results <- mapM (bimapM pure processMessage  . toTuple) validMessages
      case NE.partition' (shouldDelete . snd) results of
        This resultsToDelete    -> deleteMessages' resultsToDelete
        That resultsNotToDelete -> pure $ snd <$> resultsNotToDelete
        These resultsToDelete resultsNotToDelete -> do
          let resultsNotDeleted = snd <$> resultsNotToDelete
          deleteResults <- deleteMessages' resultsToDelete
          pure $ deleteResults <> resultsNotDeleted

    toTuple :: SqsMessage a -> (ReceiptHandle, a)
    toTuple SqsMessage{..} = (_smReceiptHandle, _smBody)

    deleteMessages' :: NonEmpty (ReceiptHandle, b) -> m (NonEmpty b)
    deleteMessages' resultsToDelete = do
      let receiptHandles = fst <$> resultsToDelete
      deleteResponses <- sqsDeleteMessages queueUri receiptHandles
      pure $ uncurry (matchDeleteResponse deleteResponses) <$> resultsToDelete

    matchDeleteResponse :: NonEmpty (ReceiptHandle, SqsResponse) -> ReceiptHandle -> b -> b
    matchDeleteResponse deleteResponses receiptHandle result =
      case snd <$> List.find ((receiptHandle ==) . fst) (NE.toList deleteResponses) of
        Nothing                   -> fromDeleteFailure SqsNoResponse result
        Just (SqsFailure failure) -> fromDeleteFailure failure result
        Just SqsSuccess           -> result

sendMessage :: (AWSConstraint r m, ToJSON a) => URI -> a -> m ()
sendMessage uri = void . Aws.send . Sqs.sendMessage (toText uri) . toJson

sendMessages :: forall r m a.
  (AWSConstraint r m, ToJSON a, Eq a) => URI -> NonEmpty a -> m (NonEmpty (a, SqsResponse))
sendMessages uri = batchRequests 10 sendBatch'
  where
    sendBatch' :: NonEmpty (SqsId, a) -> m ([SqsId], [BatchResultErrorEntry])
    sendBatch' messagesWithId = do
      let request = sendMessageBatch (toText uri)
            & smbEntries .~ NE.toList (toBatchEntry <$> messagesWithId)
      response <- Aws.send request
      let successIds = response ^.. smbrsSuccessful . each . smbreId
      pure (successIds, response ^. smbrsFailed)

    toBatchEntry :: ToJSON a => (SqsId, a) -> SendMessageBatchRequestEntry
    toBatchEntry (sqsId, a) = sendMessageBatchRequestEntry sqsId (toJson a)

receiveMessages :: (AWSConstraint r m, FromJSON a) =>
  SqsContext -> URI -> m [Either SqsInvalidMessage (SqsMessage a)]
receiveMessages SqsContext{..} uri = do
    let request =
          receiveMessage (toText uri)
          & rmMaxNumberOfMessages ?~ fromMaybe 10 _scMaxReceiveMessages
          & rmWaitTimeSeconds ?~ fromMaybe 20 _scPollWaitTimeSeconds
    response <- Aws.send request
    return $ response ^. rmrsMessages & each %~ messageToSqsMessage
  where
    messageToSqsMessage :: FromJSON a => Message -> Either SqsInvalidMessage (SqsMessage a)
    messageToSqsMessage message =
      first (messageToSqsInvalidMessage message) $ do
        _smReceiptHandle <- note "No Receipt Handle" $ ReceiptHandle <$> message ^. mReceiptHandle
        rawBody          <- note "No Body" $ message ^. mBody
        _smBody          <- fromJson rawBody
        return SqsMessage{..}

    messageToSqsInvalidMessage :: Message -> Text -> SqsInvalidMessage
    messageToSqsInvalidMessage message e =
      SqsInvalidMessage e (message ^. mBody) (message ^. mReceiptHandle) (message ^. mMessageId)

deleteMessage :: AWSConstraint r m => URI -> ReceiptHandle -> m ()
deleteMessage uri ReceiptHandle{..} =
  void $ Aws.send $ Sqs.deleteMessage (toText uri) _receiptHandle

deleteMessages :: forall r m.
  AWSConstraint r m => URI -> NonEmpty ReceiptHandle -> m (NonEmpty (ReceiptHandle, SqsResponse))
deleteMessages uri = batchRequests 10 deleteMessages'
  where
    deleteMessages' :: NonEmpty (SqsId, ReceiptHandle) -> m ([SqsId], [BatchResultErrorEntry])
    deleteMessages' receiptHandlesWithId = do
      let request = deleteMessageBatch (toText uri)
            & dmbEntries .~ NE.toList (uncurry toBatchEntry <$> receiptHandlesWithId)
      response <- Aws.send request
      let successIds = response ^.. dmbrsSuccessful . each . dId
      pure (successIds, response ^. dmbrsFailed)

    toBatchEntry :: SqsId -> ReceiptHandle -> DeleteMessageBatchRequestEntry
    toBatchEntry sqsId ReceiptHandle{..} = deleteMessageBatchRequestEntry sqsId _receiptHandle

-- | This function allows you to use a function that only allows a max number of messages, and
-- batches requests to this max size. For example, sendBatch will only allow you to send 10
-- messages at a time, but this function will allow you to call that function with larger
-- total message counts.
batchRequests :: forall a m.
  (Monad m, Eq a)
  => Int
  -> (NonEmpty (SqsId, a) -> m ([SqsId], [BatchResultErrorEntry]))
  -> NonEmpty a
  -> m (NonEmpty (a, SqsResponse))
batchRequests batchMaxSize f messages = do
  (successIds, unsuccessful) <- NE.batchesOfM batchMaxSize f messagesWithId
  return $ uncurry (toBatchResponse successIds unsuccessful) <$> messagesWithId
  where
    toBatchResponse ::
      [SqsId] -> [BatchResultErrorEntry] -> SqsId -> a -> (a, SqsResponse)
    toBatchResponse successIds errorEntries sqsId a
      | sqsId `elem` successIds = (a, SqsSuccess)
      | Just entry <- List.find ((sqsId ==) . view breeId) errorEntries =
        (a, SqsFailure $ SqsErrorEntry $ toSqsError entry)
      | otherwise = (a, SqsFailure SqsUnknownId)

    toSqsError :: BatchResultErrorEntry -> SqsError
    toSqsError errorEntry =
      SqsError (errorEntry ^. breeMessage) (errorEntry ^. breeSenderFault) (errorEntry ^. breeCode)

    messagesWithId :: NonEmpty (SqsId, a)
    messagesWithId = NE.zipWith (\a i -> (toText i, a)) messages ((0 :: Int) :| [1..])

-- | When batching requests to SQS, this function will retry any failures using the supplied
--   retry policy.
retryBatchFailures :: forall a m.
  MonadIO m
  => RetryPolicyM m
  -> (NonEmpty a -> m (NonEmpty (a, SqsResponse)))
  -> NonEmpty a
  -> m (NonEmpty (a, SqsResponse))
retryBatchFailures retryPolicy f allRequests = loop defaultRetryStatus allRequests
  where
    loop :: RetryStatus -> NonEmpty a -> m (NonEmpty (a, SqsResponse))
    loop retryStatus requests =
      (NE.partition' ((== SqsSuccess) . snd) <$> f requests) >>= \case
        This successes -> pure successes
        That failures -> retryFailures failures
        These successes failures -> (<>) successes <$> retryFailures failures
      where
        retryFailures :: NonEmpty (a, SqsResponse) -> m (NonEmpty (a, SqsResponse))
        retryFailures failures = applyAndDelay retryPolicy retryStatus >>= \case
          Just newRetryStatus -> loop newRetryStatus $ fst <$> failures
          Nothing -> pure failures

toJson :: ToJSON a => a -> Text
toJson = decodeUtf8 . toStrict . encode

fromJson :: FromJSON a => Text -> Either Text a
fromJson = first Text.pack . eitherDecode . fromStrict . encodeUtf8
