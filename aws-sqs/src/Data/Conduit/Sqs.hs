module Data.Conduit.Sqs
    ( dropSqsFailure
    ) where

import           Control.Monad.AWS.Sqs (SqsFailure, SqsResponse(..))
import           Control.Monad.Trans (lift)
import           Data.Conduit (ConduitT, awaitForever, yield)

dropSqsFailure :: Monad m => ((a, SqsFailure) -> m ()) -> ConduitT (a, SqsResponse) a m ()
dropSqsFailure f = awaitForever $ \(a, sqsResponse) -> case sqsResponse of
  SqsSuccess         -> yield a
  SqsFailure failure -> lift $ f (a, failure)
