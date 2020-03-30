{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Control.Monad.Trans.AWS.Extra where

import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.Trans (lift)
import           Control.Monad.Trans.AWS (AWST')

-- FIXME: Temporary orphan until either https://github.com/brendanhay/amazonka/pull/471
-- or https://github.com/brendanhay/amazonka/pull/510 are merged into Amazonka
instance PrimMonad m => PrimMonad (AWST' r m) where
    type PrimState (AWST' r m) = PrimState m
    primitive f = lift (primitive f)
