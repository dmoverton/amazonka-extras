{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}


module Control.Monad.AWS.Sns.Mock
    ( MockSnsT(..)
    , runMockSnsT
    , execMockSnsT
    , evalMockSnsT
    , MockPublish
    , mockPublishAnyOk
    , MockSnsCall(..)
    ) where

import           Control.Lens (makeLenses, use, (%=))
import           Control.Monad.Catch (MonadCatch, MonadThrow(..))
import           Control.Monad.Except (MonadError)
import           Control.Monad.State.Strict (MonadState, StateT(..), runStateT)
import           Control.Monad.Trans (MonadIO, MonadTrans(..))
import           Data.Bifunctor (second)
import           Data.Text (Text)

import           Control.Monad.AWS.Sns

data MockContext m = MockContext
  { _mcPublish     :: MockPublish m
  , _mcActualCalls :: [MockSnsCall]}

type MockPublish m = TopicArn -> Text -> m ()

data MockSnsCall =
  MockPublish TopicArn Text
  deriving (Show, Eq)

makeLenses ''MockContext

newtype MockSnsT m a = MockSnsT (StateT (MockContext m) m a)
  deriving ( Applicative
           , Functor
           , Monad
           , MonadCatch
           , MonadError e
           , MonadIO
           , MonadState (MockContext m)
           , MonadThrow )

instance MonadTrans MockSnsT where
  lift = MockSnsT . lift

instance MonadThrow m => MonadSns (MockSnsT m) where
  snsPublish topicArn message = MockSnsT $ do
    mcActualCalls %= (MockPublish topicArn message :)
    publish <- use mcPublish
    lift $ publish topicArn message

runMockSnsT :: MonadThrow m
  => MockPublish m
  -> MockSnsT m a
  -> m (a,[MockSnsCall])
runMockSnsT mockPublish (MockSnsT s) =
  second (reverse . _mcActualCalls) <$> runStateT s context
  where
    context = MockContext mockPublish []

execMockSnsT :: MonadThrow m
  => MockPublish m
  -> MockSnsT m a
  -> m [MockSnsCall]
execMockSnsT mockPublish = fmap snd . runMockSnsT mockPublish

evalMockSnsT :: MonadThrow m
  => MockPublish m
  -> MockSnsT m a
  -> m a
evalMockSnsT mockPublish = fmap fst . runMockSnsT mockPublish

mockPublishAnyOk :: Monad m => MockPublish m
mockPublishAnyOk _ _ = pure ()
