module Control.Monad.AWS.SqsSpec (spec) where

import           Control.Monad.State.Strict (modify', runStateT)
import           Control.Retry (constantDelay, limitRetries)
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Test.Hspec
import           Test.Hspec.QuickCheck (prop)
import           Test.QuickCheck (Positive(..), (==>))

import           Control.Monad.AWS.Sqs

spec :: Spec
spec = do

  context "batchRequests" $ do

    prop "Should batch calls" $ \batchSize messagesSize -> batchSize > 0 && messagesSize > 0 ==> do
      let allSucceed messages' = do
            modify' (+ 1)
            pure . (, []) . NE.toList $ fst <$> messages'
          messages = NE.fromList $ take messagesSize [(0::Int)..]
          expectedTimesCalled = (ceiling :: Double -> Int) $
            fromIntegral messagesSize / fromIntegral batchSize

      (result, timesCalled) <- flip runStateT (0::Int) $ batchRequests batchSize allSucceed messages
      result `shouldBe` NE.zip messages (SqsSuccess :| repeat SqsSuccess)
      timesCalled `shouldBe` expectedTimesCalled

    prop "Should not exceed max batch size" $
      \batchSize messagesSize -> batchSize > 0 && messagesSize > 0 ==> do
        let allSucceed messages' = do
              modify' (length messages' :)
              pure . (, []) . NE.toList $ fst <$> messages'
            messages = NE.fromList $ take messagesSize [(0::Int)..]

        (result, batchSizes) <- flip runStateT ([]::[Int]) $
          batchRequests batchSize allSucceed messages
        result `shouldBe` NE.zip messages (SqsSuccess :| repeat SqsSuccess)
        batchSizes `shouldSatisfy` all (<= batchSize)

  context "retryBatchFailures" $ do

    let
      makeUniqueItems :: Positive Int -> NonEmpty Int
      makeUniqueItems (Positive size) = NE.fromList [1 .. size]

      processOneItem :: Monad m => NonEmpty a -> m (NonEmpty (a, SqsResponse))
      processOneItem (first :| others) =
        pure $ (first, SqsSuccess) :| ((, SqsFailure SqsNoResponse) <$> others)

    prop "Should process all items if they all process first time" $ \size -> do
      let uniqueItems = makeUniqueItems size
          retryPolicy = constantDelay 0
          processAllItems = pure . fmap (, SqsSuccess)
      result <- retryBatchFailures retryPolicy processAllItems uniqueItems
      result `shouldBe` ((, SqsSuccess) <$> uniqueItems)

    prop "Should process all items eventually without limited retires" $ \size -> do
      let uniqueItems = makeUniqueItems size
          retryPolicy = constantDelay 0
      result <- retryBatchFailures retryPolicy processOneItem uniqueItems
      result `shouldBe` ((, SqsSuccess) <$> uniqueItems)

    prop "Should return failures if we run out of retires" $ \size' -> do
      let size@(Positive positiveSize) = (+ 1) <$> size' -- ∴ size > 1
          uniqueItems = makeUniqueItems size
          retryPolicy = constantDelay 0 <> limitRetries (positiveSize - 2)
          expectedSuccesses = (, SqsSuccess) <$> NE.init uniqueItems
          expectedFailures = [(NE.last uniqueItems, SqsFailure SqsNoResponse)]
      result <- retryBatchFailures retryPolicy processOneItem uniqueItems
      result `shouldBe` NE.fromList (expectedSuccesses <> expectedFailures)

    prop "Should return all failures if no items are processed" $
      \size@(Positive positiveSize) -> do
        let uniqueItems = makeUniqueItems size
            retryPolicy = constantDelay 0 <> limitRetries positiveSize
            processNoItems = pure . fmap (, SqsFailure SqsNoResponse)
        result <- retryBatchFailures retryPolicy processNoItems uniqueItems
        result `shouldBe` ((, SqsFailure SqsNoResponse) <$> uniqueItems)
