{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Monad.AWS.Sqs.Test
    ( MockSqsRequest(..)
    , MockSqsResponse(..)
    , runMockSqsT
    , execMockSqsT
    , evalMockSqsT
    , sqsRsFailure
    ) where

import           Control.Exception (Exception, PatternMatchFail(..), SomeException(..))
import           Control.Lens ((&), (.~), (<>~))
import           Control.Lens.TH (makeLensesFor)
import           Control.Monad.Base (MonadBase)
import           Control.Monad.Catch (MonadCatch, MonadThrow(..))
import           Control.Monad.Except (MonadError)
import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.Reader (MonadReader(..))
import           Control.Monad.State (StateT, evalStateT, execStateT, get, modify, runStateT)
import           Control.Monad.Trans (MonadIO, MonadTrans(..))
import           Data.Aeson
import           Data.Bifunctor (second)
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import           Data.Semigroup ((<>))
import qualified Data.Text as Text
import           Network.Address.URI (URI)

import           Control.Monad.AWS.Sqs

data MockContext =
  MockContext
    { _mcExpectedCalls :: [(MockSqsRequest, MockSqsResponse)]
    , _mcActualCalls   :: [MockSqsRequest] }

data MockSqsRequest =
    SqsRqSend    URI (NonEmpty Value)
  | SqsRqReceive URI
  | SqsRqDelete  URI (NonEmpty ReceiptHandle)
  deriving (Eq, Show)

data MockSqsResponse =
    SqsRsMessages [Either SqsInvalidMessage (SqsMessage Value)]
  | SqsRsMany     (NonEmpty SqsResponse)
  | SqsRsFailure  SomeException

makeLensesFor [("_mcActualCalls", "mcActualCalls")] ''MockContext

newtype MockSqsT m a =
  MockSqsT (StateT MockContext m a)
  deriving ( Applicative
           , Functor
           , MFunctor
           , Monad
           , MonadBase b'
           , MonadCatch
           , MonadError e
           , MonadIO
           , MonadReader r
           , MonadThrow
           , MonadTrans )

instance MonadThrow m => MonadSqs (MockSqsT m) where
  sqsSendMessage     = mockSendMessage
  sqsSendMessages    = mockSendMessages
  sqsReceiveMessages = mockReceiveMessages
  sqsDeleteMessage   = mockDeleteMessage
  sqsDeleteMessages  = mockDeleteMessages

instance PrimMonad m => PrimMonad (MockSqsT m) where
  type PrimState (MockSqsT m) = PrimState m
  primitive f = MockSqsT $ primitive f

runMockSqsT ::
  MonadThrow m
  => [(MockSqsRequest, MockSqsResponse)]
  -> MockSqsT m a
  -> m (a, [MockSqsRequest])
runMockSqsT expectedCalls (MockSqsT s) =
  second _mcActualCalls <$> runStateT s (MockContext expectedCalls [])

execMockSqsT ::
  MonadThrow m
  => [(MockSqsRequest, MockSqsResponse)]
  -> MockSqsT m a
  -> m [MockSqsRequest]
execMockSqsT expectedCalls (MockSqsT s) =
  _mcActualCalls <$> execStateT s (MockContext expectedCalls [])

evalMockSqsT ::
  MonadThrow m
  => [(MockSqsRequest, MockSqsResponse)]
  -> MockSqsT m a
  -> m a
evalMockSqsT expectedCalls (MockSqsT s) = evalStateT s $ MockContext expectedCalls []

sqsRsFailure :: Exception e => e -> MockSqsResponse
sqsRsFailure e = SqsRsFailure $ SomeException e

mockSendMessage :: (MonadThrow m, ToJSON a) => URI -> a -> MockSqsT m ()
mockSendMessage uri a = do
  response <- matchTestCall $ SqsRqSend uri [toJSON a]
  case response of
    SqsRsMany [SqsSuccess] -> pure ()
    _                      -> throwM $ patternMatchFail "sqsSendMessage"

mockSendMessages ::
  (MonadThrow m, Eq a, ToJSON a) => URI -> NonEmpty a -> MockSqsT m (NonEmpty (a, SqsResponse))
mockSendMessages uri as = do
  response <- matchTestCall $ SqsRqSend uri $ toJSON <$> as
  case response of
    SqsRsMany sendResponses -> pure $ NE.zip as sendResponses
    _                       -> throwM $ patternMatchFail "sqsSendMessages"

mockReceiveMessages :: forall m a.
  (MonadThrow m, FromJSON a)
  => URI
  -> MockSqsT m [Either SqsInvalidMessage (SqsMessage a)]
mockReceiveMessages uri = do
  response <- matchTestCall $ SqsRqReceive uri
  case response of
    SqsRsMessages sqsMessages -> pure $ (>>= tryParseJson) <$> sqsMessages
    _                         -> throwM $ patternMatchFail "sqsReceiveMessages"
  where
    tryParseJson :: SqsMessage Value -> Either SqsInvalidMessage (SqsMessage a)
    tryParseJson sqsMessage@SqsMessage{..} =
      case fromJSON _smBody of
        Success a -> Right $ sqsMessage & smBody .~ a
        Error   e -> Left $ SqsInvalidMessage (Text.pack e)
                                              (Just . Text.pack . show $ _smBody)
                                              (Just $ _receiptHandle _smReceiptHandle)
                                              Nothing

mockDeleteMessage :: MonadThrow m => URI -> ReceiptHandle -> MockSqsT m ()
mockDeleteMessage uri receiptHandle = do
  response <- matchTestCall $ SqsRqDelete uri [receiptHandle]
  case response of
    SqsRsMany [SqsSuccess] -> pure ()
    _                      -> throwM $ patternMatchFail "sqsDeleteMessage"

mockDeleteMessages ::
  MonadThrow m
  => URI
  -> NonEmpty ReceiptHandle
  -> MockSqsT m (NonEmpty (ReceiptHandle, SqsResponse))
mockDeleteMessages uri receiptHandles = do
  response <- matchTestCall $ SqsRqDelete uri receiptHandles
  case response of
    SqsRsMany deleteResponses -> pure $ NE.zip receiptHandles deleteResponses
    _                         -> throwM $ patternMatchFail "sqsDeleteMessages"

matchTestCall :: MonadThrow m => MockSqsRequest -> MockSqsT m MockSqsResponse
matchTestCall request = do
  MockContext{..} <- MockSqsT get
  MockSqsT $ modify (mcActualCalls <>~ [request])
  case lookup request _mcExpectedCalls of
    Nothing                               -> pure $ SqsRsMany [SqsSuccess]
    Just (SqsRsFailure (SomeException e)) -> throwM e
    Just response                         -> pure response

patternMatchFail :: String -> PatternMatchFail
patternMatchFail functionName =
  PatternMatchFail $ "Missing or incorrect SQS mock " <> functionName <> " response type"
