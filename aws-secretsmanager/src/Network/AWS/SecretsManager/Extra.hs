module Network.AWS.SecretsManager.Extra
    ( getSecret
    ) where

import           Control.Lens (to, (^?), _Just)
import           Control.Monad.Trans.AWS (AWSConstraint, send)
import           Data.Aeson (FromJSON, eitherDecodeStrict)
import           Data.Bifunctor (first)
import           Data.Text.Encoding (encodeUtf8)
import           Data.Text1 (Text1, toText, unpack)
import           Network.AWS.SecretsManager (getSecretValue, gsvrsSecretString)

getSecret :: (AWSConstraint r m, FromJSON a) => Text1 -> m (Either String a)
getSecret secretName = do
  secretResponse <- send $ getSecretValue $ toText secretName
  pure $ case secretResponse ^? gsvrsSecretString . _Just . to encodeUtf8 of
    Nothing    -> Left $ "No secret found for " <> unpack secretName
    Just bytes -> first toDecodeError $ eitherDecodeStrict bytes
  where
    toDecodeError err =  "Invalid secret for " <> unpack secretName <> ": " <> err
