{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE UndecidableInstances #-}

module Control.Monad.AWS.Sns
    ( MonadSns(..)
    , snsPublishJSON
    , TopicArn(..)
    ) where

import           Control.Lens ((&), (?~))
import           Control.Monad (void)
import           Control.Monad.Catch (MonadCatch(..))
import           Control.Monad.Trans (MonadTrans(..))
import           Control.Monad.Trans.AWS (AWST', HasEnv, send)
import           Control.Monad.Trans.Resource (MonadResource)
import           Data.Aeson (FromJSON, ToJSON)
import           Data.Aeson.Text (encodeToLazyText)
import           Data.String (IsString)
import           Data.Text (Text)
import           Data.Text.Lazy (toStrict)
import           GHC.Generics (Generic)
import           Network.AWS.SNS (pTopicARN, publish)
import           Test.QuickCheck (Arbitrary)
import           Test.QuickCheck.Instances ()

newtype TopicArn = TopicArn { _topicArn :: Text }
  deriving (Show, Eq, Generic)
  deriving newtype (Arbitrary, IsString, ToJSON, FromJSON)

class Monad m => MonadSns m where
  snsPublish :: TopicArn -> Text -> m ()

instance {-# OVERLAPPABLE #-} (MonadTrans t, Monad (t m), MonadSns m) => MonadSns (t m) where
  snsPublish topicArn = lift . snsPublish topicArn

instance (MonadCatch m, MonadResource m, HasEnv r) => MonadSns (AWST' r m) where
  snsPublish TopicArn{..} message =
    void $ send $ publish message & pTopicARN ?~ _topicArn

snsPublishJSON :: (MonadSns m, ToJSON a) => TopicArn -> a -> m ()
snsPublishJSON topicArn = snsPublish topicArn . toStrict . encodeToLazyText
