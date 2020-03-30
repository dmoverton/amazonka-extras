module Control.Monad.AWS.Sns.MockSpec (spec) where

import           Test.Hspec

import           Control.Monad.AWS.Sns
import           Control.Monad.AWS.Sns.Mock

spec :: Spec
spec =

  context "snsPublish" $ do

    it "Should mock snsPublish correctly" $ do
      result <- evalMockSnsT mockPublishAnyOk $ snsPublish (TopicArn "A::Topic") "True"
      result `shouldBe` ()

    it "Should record mock snsPublish call" $ do
      let topic = (TopicArn "A::Topic")
      calls <- execMockSnsT mockPublishAnyOk $ do
        snsPublish topic "A message"
      calls `shouldBe` [MockPublish topic "A message"]

    it "Should record mock snsPublish calls in order" $ do
      let topic = (TopicArn "A::Topic")
      calls <- execMockSnsT mockPublishAnyOk $ do
        snsPublish topic "False"
        snsPublish topic "True"
      calls `shouldBe` [MockPublish topic "False", MockPublish topic "True"]
