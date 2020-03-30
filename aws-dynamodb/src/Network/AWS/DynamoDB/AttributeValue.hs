{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Network.AWS.DynamoDB.AttributeValue
    ( AttributeValues(..)
    , ToAttributeValue(..)
    , toAttributeValues'
    , FromAttributeValue(..)
    , fromAttributeValues'
    , DynamoObject
    , (..:)
    , (..:?)
    , (..!=)
    , (..?)
    , (..=)
    , fromDynamoList
    , parseAttributeValue
    , toText1AttributeValue
    , fromText1AttributeValue
    , toText1SetAttributeValue
    , fromText1SetAttributeValue
    , toText1Keys
    , fromText1Keys
    , filterNullAttributeValues
    , isAttributeValueNull
    , Parser
    , parseMaybe
    , parseEither
    ) where

import           Control.Applicative (Alternative(..), liftA2, optional)
import           Control.DeepSeq (NFData(..))
import           Control.Lens (has, view, (&), (.~), (?~), _Just)
import           Control.Monad (MonadPlus(..), ap, (<=<))
import qualified Control.Monad.Fail as Fail
import           Control.Newtype.Generics (Newtype(O))
import qualified Control.Newtype.Generics as Newtype
import           Data.Aeson (ToJSON(..), Value(..))
import           Data.Bifunctor (bimap, first)
import           Data.Bitraversable (bitraverse)
import           Data.ByteString (ByteString)
import           Data.Char (isAlpha, isAlphaNum)
import           Data.Fixed (Fixed, HasResolution)
import           Data.Hashable (Hashable)
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import           Data.Int (Int64)
import           Data.IntSet (IntSet)
import qualified Data.IntSet as IS
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe, mapMaybe)
import           Data.Scientific (Scientific)
import           Data.Semigroup (Semigroup(..))
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Text (Text, toUpper)
import           Data.Text1
                  (AsText1(..), FromText1, Text1, ToText1(..), fromText1M, text1ToText,
                  textToText1M, toText)
import qualified Data.Text1 as Text1
-- import           Data.Thyme (UTCTime(..))
import           Data.Typeable (Typeable)
import           Data.UUID.Types (UUID)
import qualified Data.Vector as V
import           Network.Address.Email (EmailAddress)
import           Network.Address.URI (URI)
import           Network.AWS.Data.Numeric (Nat)
import qualified Network.AWS.Data.Text as AWS
import           Network.AWS.DynamoDB (AttributeValue)
import qualified Network.AWS.DynamoDB as Ddb
import           Numeric.Natural (Natural)


-- | Retrieve the value associated with the given key of a 'DynamoObject'.
-- The result is 'empty' if the key is not present or the value cannot
-- be converted to the desired type.
--
-- This accessor is appropriate if the key and value /must/ be present
-- in an object for it to be valid.  If the key and value are
-- optional, use '..:?' instead.
(..:) :: FromAttributeValue a => DynamoObject -> Text1 -> Parser a
(..:) = explicitParseField fromAttributeValue

-- | Retrieve the value associated with the given key of a 'DynamoObject'. The
-- result is 'Nothing' if the key is not present or if its value is 'NULL',
-- or 'empty' if the value cannot be converted to the desired type.
--
-- This accessor is most useful if the key and value can be absent
-- from an object without affecting its validity.  If the key and
-- value are mandatory, use '..:' instead.
-- (..:?) :: FromAttributeValue a => HashMap Text AttributeValue -> Text -> Parser (Maybe a)
-- (..:?) = explicitParseFieldMaybe fromAttributeValue
(..:?) :: FromAttributeValue a => DynamoObject -> Text1 -> Parser (Maybe a)
(..:?) = explicitParseFieldMaybe fromAttributeValue

-- | Variant of '..:' with explicit parser function.
explicitParseField ::
  (AttributeValue -> Parser a) -> DynamoObject -> Text1 -> Parser a
explicitParseField p obj key = case HashMap.lookup key obj of
  Nothing    -> fail $ "key " ++ show key ++ " not present"
  Just value -> p value <?> Key key

-- | Variant of '..:?' with explicit parser function.
explicitParseFieldMaybe :: (AttributeValue -> Parser a) -> DynamoObject -> Text1 -> Parser (Maybe a)
explicitParseFieldMaybe p obj key = case HashMap.lookup key obj of
  Nothing -> pure Nothing
  Just av -> case fromAV av of
    DynamoNull -> pure Nothing
    _          -> pure <$> p av <?> Key key

-- | Helper for use in combination with '..:?' to provide default
-- values for optional Dynamo object fields.
--
-- This combinator is most useful if the key and value can be absent
-- from an object without affecting its validity and we know a default
-- value to assign in that case.  If the key and value are mandatory,
-- use '..:' instead.
--
-- Example usage:
--
-- @ v1 <- o '..:?' \"opt_field_with_dfl\" .!= \"default_val\"
-- v2 <- o '..:'  \"mandatory_field\"
-- v3 <- o '..:?' \"opt_field2\"
-- @
(..!=) :: Parser (Maybe a) -> a -> Parser a
pmval ..!= val = fromMaybe val <$> pmval

-- | Short hand for using ..:? and ..!= when the fallback value is just empty
(..?) :: (Alternative f, FromAttributeValue (f a)) => DynamoObject -> Text1 -> Parser (f a)
(..?) attributes key = attributes ..:? key ..!= empty

(..=) :: ToAttributeValue a => Text1 -> a -> DynamoPair
(..=) key a = (key, toAttributeValue a)

-- | Elements of a JSON path used to describe the location of an
-- error.
data DynamoPathElement =
    Key Text1
   -- ^ JSON path element of a key into an object,
   -- \"object.key\".
 | Index {-# UNPACK #-} !Int
   -- ^ JSON path element of an index into an
   -- array, \"array[index]\".
   deriving (Eq, Show, Typeable, Ord)

type DynamoPath = [DynamoPathElement]

-- | The internal result of running a 'Parser'.
data IResult a =
    IError DynamoPath String
  | ISuccess a
  deriving (Eq, Show, Typeable)

-- | The result of running a 'Parser'.
data Result a =
    Error String
  | Success a
  deriving (Eq, Show, Typeable)

instance NFData DynamoPathElement where
  rnf (Key t)   = rnf t
  rnf (Index i) = rnf i

instance (NFData a) => NFData (IResult a) where
  rnf (ISuccess a)      = rnf a
  rnf (IError path err) = rnf path `seq` rnf err

instance (NFData a) => NFData (Result a) where
  rnf (Success a) = rnf a
  rnf (Error err) = rnf err

instance Functor IResult where
  fmap f (ISuccess a)      = ISuccess (f a)
  fmap _ (IError path err) = IError path err
  {-# INLINE fmap #-}

instance Functor Result where
  fmap f (Success a) = Success (f a)
  fmap _ (Error err) = Error err
  {-# INLINE fmap #-}

instance Monad IResult where
  return = pure
  {-# INLINE return #-}

  ISuccess a      >>= k = k a
  IError path err >>= _ = IError path err
  {-# INLINE (>>=) #-}

  fail = Fail.fail
  {-# INLINE fail #-}

instance Fail.MonadFail IResult where
  fail = IError []
  {-# INLINE fail #-}

instance Monad Result where
  return = pure
  {-# INLINE return #-}

  Success a >>= k = k a
  Error err >>= _ = Error err
  {-# INLINE (>>=) #-}

  fail = Fail.fail
  {-# INLINE fail #-}

instance Fail.MonadFail Result where
  fail = Error
  {-# INLINE fail #-}

instance Applicative IResult where
  pure  = ISuccess
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}

instance Applicative Result where
  pure  = Success
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}

instance MonadPlus IResult where
  mzero = fail "mzero"
  {-# INLINE mzero #-}
  mplus a@(ISuccess _) _ = a
  mplus _ b              = b
  {-# INLINE mplus #-}

instance MonadPlus Result where
  mzero = fail "mzero"
  {-# INLINE mzero #-}
  mplus a@(Success _) _ = a
  mplus _ b             = b
  {-# INLINE mplus #-}

instance Alternative IResult where
  empty = mzero
  {-# INLINE empty #-}
  (<|>) = mplus
  {-# INLINE (<|>) #-}

instance Alternative Result where
  empty = mzero
  {-# INLINE empty #-}
  (<|>) = mplus
  {-# INLINE (<|>) #-}

instance Semigroup (IResult a) where
  (<>) = mplus
  {-# INLINE (<>) #-}

instance Monoid (IResult a) where
  mempty  = fail "mempty"
  {-# INLINE mempty #-}
  mappend = (<>)
  {-# INLINE mappend #-}

instance Semigroup (Result a) where
  (<>) = mplus
  {-# INLINE (<>) #-}

instance Monoid (Result a) where
  mempty  = fail "mempty"
  {-# INLINE mempty #-}
  mappend = (<>)
  {-# INLINE mappend #-}

instance Foldable IResult where
  foldMap _ (IError _ _) = mempty
  foldMap f (ISuccess y) = f y
  {-# INLINE foldMap #-}

  foldr _ z (IError _ _) = z
  foldr f z (ISuccess y) = f y z
  {-# INLINE foldr #-}

instance Foldable Result where
  foldMap _ (Error _)   = mempty
  foldMap f (Success y) = f y
  {-# INLINE foldMap #-}

  foldr _ z (Error _)   = z
  foldr f z (Success y) = f y z
  {-# INLINE foldr #-}

instance Traversable IResult where
  traverse _ (IError path err) = pure (IError path err)
  traverse f (ISuccess a)      = ISuccess <$> f a
  {-# INLINE traverse #-}

instance Traversable Result where
  traverse _ (Error err) = pure (Error err)
  traverse f (Success a) = Success <$> f a
  {-# INLINE traverse #-}

-- | Failure continuation.
type Failure f r   = DynamoPath -> String -> f r
-- | Success continuation.
type Success a f r = a -> f r

-- | An AttributeValue parser.  N.B. This might not fit your usual understanding of
--  "parser".  Instead you might like to think of 'Parser' as a "parse result",
-- i.e. a parser to which the input has already been applied.
newtype Parser a =
  Parser
    { runParser :: forall f r. DynamoPath -> Failure f r -> Success a f r -> f r }

instance Monad Parser where
  m >>= g = Parser $ \path kf ks -> let ks' a = runParser (g a) path kf ks
                                      in runParser m path kf ks'
  {-# INLINE (>>=) #-}
  return = pure
  {-# INLINE return #-}
  fail = Fail.fail
  {-# INLINE fail #-}

instance Fail.MonadFail Parser where
  fail msg = Parser $ \path kf _ks -> kf (reverse path) msg
  {-# INLINE fail #-}

instance Functor Parser where
  fmap f m = Parser $ \path kf ks ->
    let ks' a = ks (f a)
    in runParser m path kf ks'
  {-# INLINE fmap #-}

instance Applicative Parser where
  pure a = Parser $ \_path _kf ks -> ks a
  {-# INLINE pure #-}
  (<*>) = apP
  {-# INLINE (<*>) #-}

instance Alternative Parser where
  empty = fail "empty"
  {-# INLINE empty #-}
  (<|>) = mplus
  {-# INLINE (<|>) #-}

instance MonadPlus Parser where
  mzero = fail "mzero"
  {-# INLINE mzero #-}
  mplus a b = Parser $ \path kf ks ->
    let kf' _ _ = runParser b path kf ks
    in runParser a path kf' ks
  {-# INLINE mplus #-}

instance Semigroup (Parser a) where
  (<>) = mplus
  {-# INLINE (<>) #-}

instance Monoid (Parser a) where
  mempty  = fail "mempty"
  {-# INLINE mempty #-}
  mappend = (<>)
  {-# INLINE mappend #-}

apP :: Parser (a -> b) -> Parser a -> Parser b
apP d e = do
  b <- d
  b <$> e
{-# INLINE apP #-}

-- | Add Dynamo Path context to a parser
--
-- When parsing a complex structure, it helps to annotate (sub)parsers
-- with context, so that if an error occurs, you can find its location.
--
-- > withObject "Person" $ \o ->
-- >   Person
-- >     <$> o .: "name" <?> Key "name"
-- >     <*> o .: "age"  <?> Key "age"
--
-- (Standard methods like '(.:)' already do this.)
--
-- With such annotations, if an error occurs, you will get a Dynamo Path
-- location of that error.
(<?>) :: Parser a -> DynamoPathElement -> Parser a
p <?> pathElem = Parser $ \path kf ks -> runParser p (pathElem:path) kf ks

-- | Run a 'Parser' with a 'Maybe' result type.
parseMaybe :: (a -> Parser b) -> a -> Maybe b
parseMaybe m v = runParser (m v) [] (\_ _ -> Nothing) Just
{-# INLINE parseMaybe #-}

-- | Run a 'Parser' with an 'Either' result type.  If the parse fails,
-- the 'Left' payload will contain an error message.
parseEither :: (a -> Parser b) -> a -> Either String b
parseEither m v = runParser (m v) [] onError Right
  where onError path msg = Left (formatError path msg)
{-# INLINE parseEither #-}

-- | Annotate an error message with a DynamoPath error location.
--   Overt your eyes.
formatError :: DynamoPath -> String -> String
formatError path msg = "Error in " ++ format "$" path ++ ": " ++ msg
  where
    format :: String -> DynamoPath -> String
    format pfx []                = pfx
    format pfx (Index idx:parts) = format (pfx ++ "[" ++ show idx ++ "]") parts
    format pfx (Key key:parts)   = format (pfx ++ formatKey key) parts

    formatKey :: Text1 -> String
    formatKey key
       | isIdentifierKey strKey = "." ++ strKey
       | otherwise              = "['" ++ escapeKey strKey ++ "']"
      where strKey = Text1.unpack key

    isIdentifierKey :: String -> Bool
    isIdentifierKey []     = False
    isIdentifierKey (x:xs) = isAlpha x && all isAlphaNum xs

    escapeKey :: String -> String
    escapeKey = concatMap escapeChar

    escapeChar :: Char -> String
    escapeChar '\'' = "\\'"
    escapeChar '\\' = "\\\\"
    escapeChar c    = [c]

-- | A key/value pair for an Object.
type DynamoPair = (Text1, AttributeValue)
type DynamoObject = HashMap Text1 AttributeValue
type DynamoList = [AttributeValue]

fromDynamoList :: [DynamoPair] -> DynamoObject
fromDynamoList = HashMap.fromList

parseAttributeValue :: FromAttributeValue a => AttributeValue -> Either String a
parseAttributeValue = parseEither fromAttributeValue

fromText1AttributeValue :: FromText1 a => AttributeValue -> Parser a
fromText1AttributeValue = fromText1M <=< fromAttributeValue

toText1AttributeValue :: ToText1 a => a -> AttributeValue
toText1AttributeValue = toAttributeValue . toText1

fromText1SetAttributeValue :: (FromText1 a, Ord a) => AttributeValue -> Parser (Set a)
fromText1SetAttributeValue = traverseSet fromText1M <=< fromAttributeValue

traverseSet :: (Applicative f, Ord b) => (a -> f b) -> Set a -> f (Set b)
traverseSet f = Set.foldr consF $ pure Set.empty
  where consF x = liftA2 Set.insert $ f x

toText1SetAttributeValue :: ToText1 a => Set a -> AttributeValue
toText1SetAttributeValue = toAttributeValue . Set.map toText1

class AttributeValues a where
  toAttributeValues :: a -> DynamoObject
  fromAttributeValues :: DynamoObject -> Parser a

instance AttributeValues DynamoObject where
  toAttributeValues = id
  fromAttributeValues = pure

instance
  {-# OVERLAPPABLE #-}
  (ToText1 k, FromText1 k, Ord k, Hashable k, ToAttributeValue a, FromAttributeValue a)
  => AttributeValues (HashMap k a)
  where
    toAttributeValues = toDynamoObject . HashMap.toList
    fromAttributeValues = fmap HashMap.fromList . fromDynamoObject

instance
  {-# OVERLAPPABLE #-}
  (ToText1 k, FromText1 k, Ord k, ToAttributeValue a, FromAttributeValue a)
  => AttributeValues (Map k a)
  where
    toAttributeValues = toDynamoObject . Map.toList
    fromAttributeValues = fmap Map.fromList . fromDynamoObject

toAttributeValues' :: AttributeValues a => a -> HashMap Text AttributeValue
toAttributeValues' = fromText1Keys . filterNullAttributeValues . toAttributeValues

fromAttributeValues' :: AttributeValues a => HashMap Text AttributeValue -> Parser a
fromAttributeValues' = fromAttributeValues . toText1Keys

filterNullAttributeValues :: DynamoObject -> DynamoObject
filterNullAttributeValues = HashMap.filter (not . isAttributeValueNull)

isAttributeValueNull :: AttributeValue -> Bool
isAttributeValueNull = has $ Ddb.avNULL . _Just

fromText1Keys :: HashMap Text1 a -> HashMap Text a
fromText1Keys = HashMap.fromList . fmap (first text1ToText) . HashMap.toList

toText1Keys :: HashMap Text a -> HashMap Text1 a
toText1Keys = HashMap.fromList . mapMaybe (bitraverse textToText1M Just) . HashMap.toList

class FromAttributeValue a where
  fromAttributeValue :: AttributeValue -> Parser a

instance {-# OVERLAPPABLE #-} AttributeValues a => FromAttributeValue a where
  fromAttributeValue = withObject "Object" fromAttributeValues . fromAV

instance FromAttributeValue a => FromAttributeValue (Maybe a) where
  fromAttributeValue a = case fromAV a of
    DynamoNull -> pure Nothing
    _          -> optional $ fromAttributeValue a

instance
  {-# OVERLAPPABLE #-}
  (FromText1 k, Ord k, Hashable k, FromAttributeValue a)
  => FromAttributeValue (HashMap k a)
  where
    fromAttributeValue = withObject "Object" (fmap HashMap.fromList . fromDynamoObject) . fromAV

instance
  {-# OVERLAPPABLE #-}
  (FromText1 k, Ord k, FromAttributeValue a)
  => FromAttributeValue (Map k a)
  where
    fromAttributeValue = withObject "Object" (fmap Map.fromList . fromDynamoObject) . fromAV

fromDynamoObject :: (FromText1 k, FromAttributeValue a) => DynamoObject -> Parser [(k, a)]
fromDynamoObject = traverse (bitraverse fromText1M fromAttributeValue) . HashMap.toList

instance {-# OVERLAPPABLE #-} FromAttributeValue [a] => FromAttributeValue (NonEmpty a) where
  fromAttributeValue = maybe (fail "List is empty") pure . NonEmpty.nonEmpty <=< fromAttributeValue

instance {-# OVERLAPPABLE #-} FromAttributeValue a => FromAttributeValue [a] where
  fromAttributeValue = withList "List" (traverse fromAttributeValue) . fromAV

instance {-# OVERLAPPING #-} FromAttributeValue IntSet where
  fromAttributeValue =
    fmap IS.fromList . traverse fromText1M . mapMaybe textToText1M . view Ddb.avNS

instance {-# OVERLAPPABLE #-}
    (Newtype n, Ord n, FromAttributeValue (Set (O n))) => FromAttributeValue (Set n) where
  fromAttributeValue = fmap (Set.map Newtype.pack) . fromAttributeValue

instance {-# OVERLAPPING #-} FromAttributeValue (Set ByteString) where
  fromAttributeValue = withByteStringSet "ByteString Set" pure . fromAV

instance {-# OVERLAPPING #-} FromAttributeValue (Set Text1) where
  fromAttributeValue = withStringSet "Text Set" pure . fromAV

instance {-# OVERLAPPING #-} FromAttributeValue (Set Double) where
  fromAttributeValue = fromNumberSet

instance {-# OVERLAPPING #-} FromAttributeValue (Set Int) where
  fromAttributeValue = fromNumberSet

instance {-# OVERLAPPING #-} FromAttributeValue (Set Int64) where
  fromAttributeValue = fromNumberSet

instance {-# OVERLAPPING #-} FromAttributeValue (Set Integer) where
  fromAttributeValue = fromNumberSet

instance {-# OVERLAPPING #-} FromAttributeValue (Set Natural) where
  fromAttributeValue = fromNumberSet

instance {-# OVERLAPPING #-} FromAttributeValue (Set Nat) where
  fromAttributeValue = fromNumberSet

instance {-# OVERLAPPING #-} FromAttributeValue (Set Scientific) where
  fromAttributeValue = fromNumberSet

fromNumberSet :: (Num a, FromText1 a, Ord a) => AttributeValue -> Parser (Set a)
fromNumberSet =
  withNumberSet "Number Set" (traverseSet fromText1M) . fromAV

instance FromAttributeValue Double where
  fromAttributeValue = withNumber "Double" fromText1M . fromAV

instance FromAttributeValue Int where
  fromAttributeValue = withNumber "Int" fromText1M . fromAV

instance FromAttributeValue Int64 where
  fromAttributeValue = withNumber "Int64" fromText1M . fromAV

instance FromAttributeValue Integer where
  fromAttributeValue = withNumber "Integer" fromText1M . fromAV

instance FromAttributeValue Natural where
  fromAttributeValue = withNumber "Natural" fromText1M . fromAV

instance FromAttributeValue Nat where
  fromAttributeValue = withNumber "Nat" fromText1M . fromAV

instance FromAttributeValue Scientific where
  fromAttributeValue = withNumber "Scientific" fromText1M . fromAV

instance HasResolution a => FromAttributeValue (Fixed a) where
  fromAttributeValue = withNumber "Fixed" fromText1M . fromAV

instance FromAttributeValue Text1 where
  fromAttributeValue = withString "Text1" pure . fromAV

instance FromText1 a => FromAttributeValue (AsText1 a) where
  fromAttributeValue =  fmap AsText1 . fromText1AttributeValue

instance FromAttributeValue UUID where
  fromAttributeValue = withString "UUID" fromText1M . fromAV

instance FromAttributeValue Bool where
  fromAttributeValue = withBool "Bool" pure . fromAV

-- FIXME
-- instance FromAttributeValue UTCTime where
--   fromAttributeValue = withString "UTCTime" fromText1M . fromAV

instance FromAttributeValue ByteString where
  fromAttributeValue = withByteString "ByteString" pure . fromAV

instance FromAttributeValue URI where
  fromAttributeValue = fromText1AttributeValue

instance FromAttributeValue EmailAddress where
  fromAttributeValue = fromText1AttributeValue

instance FromAttributeValue AttributeValue where
  fromAttributeValue = pure

instance FromAttributeValue Value where
  fromAttributeValue av = case fromAV av of
    DynamoBool b            -> pure $ Bool b
    DynamoByteString bs     -> pure . String $ AWS.toText bs
    DynamoByteStringSet bss -> pure . Array . fmap (String . AWS.toText) $ setToV bss
    DynamoObject o          -> Object <$> traverse fromAttributeValue (fromText1Keys o)
    DynamoNull              -> pure Null
    DynamoNumber n          -> Number <$> fromText1M n
    DynamoNumberSet ns      -> fmap (Array . fmap Number) . traverse fromText1M $ setToV ns
    DynamoString s          -> pure $ String $ toText s
    DynamoStringSet ss      -> pure . Array . fmap (String . toText) $ setToV ss
    DynamoList as           -> Array . V.fromList <$> traverse fromAttributeValue as
    where setToV = V.fromList . Set.toList

class ToAttributeValue a where
  toAttributeValue :: a -> AttributeValue

instance {-# OVERLAPPABLE #-} AttributeValues a => ToAttributeValue a where
  toAttributeValue = toAV . DynamoObject . toAttributeValues

instance ToAttributeValue a => ToAttributeValue (Maybe a) where
  toAttributeValue = \case
    Nothing -> toAV DynamoNull
    Just a  -> toAttributeValue a

instance
  {-# OVERLAPPABLE #-}
  (ToText1 k, ToAttributeValue a)
  => ToAttributeValue (HashMap k a)
  where
    toAttributeValue = toAV . DynamoObject . toDynamoObject . HashMap.toList

instance {-# OVERLAPPABLE #-} (ToText1 k, ToAttributeValue a) => ToAttributeValue (Map k a) where
  toAttributeValue = toAV . DynamoObject . toDynamoObject . Map.toList

toDynamoObject :: (ToText1 k, ToAttributeValue v) => [(k, v)] -> DynamoObject
toDynamoObject = HashMap.fromList . fmap (bimap toText1 toAttributeValue)

instance {-# OVERLAPPABLE #-} ToAttributeValue a => ToAttributeValue [a] where
  toAttributeValue = toAV . DynamoList . fmap toAttributeValue

instance {-# OVERLAPPABLE #-} ToAttributeValue [a] => ToAttributeValue (NonEmpty a) where
  toAttributeValue = toAttributeValue . NonEmpty.toList

instance {-# OVERLAPPING #-} ToAttributeValue IntSet where
  toAttributeValue intSet
    | IS.null intSet = toAV DynamoNull
    | otherwise = Ddb.attributeValue & Ddb.avNS .~ (toText <$> IS.toList intSet)

instance {-# OVERLAPPABLE #-}
    (Newtype n, Ord (O n), ToAttributeValue (Set (O n))) => ToAttributeValue (Set n) where
  toAttributeValue = toAttributeValue . Set.map Newtype.unpack

instance {-# OVERLAPPING #-} ToAttributeValue (Set ByteString) where
  toAttributeValue = guardEmptySet $ toAV . DynamoByteStringSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Text1) where
    toAttributeValue = guardEmptySet $ toAV . DynamoStringSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Double) where
  toAttributeValue = toNumberSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Int) where
  toAttributeValue = toNumberSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Int64) where
  toAttributeValue = toNumberSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Integer) where
  toAttributeValue = toNumberSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Natural) where
  toAttributeValue = toNumberSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Nat) where
  toAttributeValue = toNumberSet

instance {-# OVERLAPPING #-} ToAttributeValue (Set Scientific) where
  toAttributeValue = toNumberSet

toNumberSet :: (Num a, ToText1 a) => Set a -> AttributeValue
toNumberSet = guardEmptySet $ toAV . DynamoNumberSet . Set.map toText1

-- | Dynamo doesnt allow empty sets
guardEmptySet :: (Set a -> AttributeValue) -> Set a -> AttributeValue
guardEmptySet f set
  | Set.null set = toAV DynamoNull
  | otherwise = f set

instance ToAttributeValue Double where
  toAttributeValue = toAV . DynamoNumber . toText1

instance ToAttributeValue Int where
  toAttributeValue = toAV . DynamoNumber . toText1

instance ToAttributeValue Int64 where
  toAttributeValue = toAV . DynamoNumber . toText1

instance ToAttributeValue Integer where
  toAttributeValue = toAV . DynamoNumber . toText1

instance ToAttributeValue Natural where
  toAttributeValue = toAV . DynamoNumber . toText1

instance ToAttributeValue Nat where
  toAttributeValue = toAV . DynamoNumber . toText1

instance ToAttributeValue Scientific where
  toAttributeValue = toAV . DynamoNumber . toText1

instance HasResolution a => ToAttributeValue (Fixed a) where
  toAttributeValue = toAV . DynamoNumber . toText1

instance ToAttributeValue Text1 where
  toAttributeValue = toAV . DynamoString

instance ToText1 a => ToAttributeValue (AsText1 a) where
  toAttributeValue = toText1AttributeValue . _asText1

instance ToAttributeValue UUID where
  toAttributeValue = toAV . DynamoString . toText1

instance ToAttributeValue Bool where
  toAttributeValue = toAV . DynamoBool

-- FIXME
-- instance ToAttributeValue UTCTime where
--   toAttributeValue = toAV . DynamoString . toText1

instance ToAttributeValue ByteString where
  toAttributeValue = \case
    "" -> toAV DynamoNull -- Dynamo doesnt like empty binary data
    b  -> toAV $ DynamoByteString b

instance ToAttributeValue URI where
  toAttributeValue = toText1AttributeValue

instance ToAttributeValue EmailAddress where
  toAttributeValue = toText1AttributeValue

instance ToAttributeValue AttributeValue where
  toAttributeValue = id

instance ToAttributeValue Value where
  toAttributeValue = \case
    Object o -> toAttributeValue $ toText1Keys o
    Array  a -> toAttributeValue $ V.toList a
    String s -> toAttributeValue $ textToText1M @Maybe s
    Number n -> toAttributeValue n
    Bool   b -> toAttributeValue b
    Null     -> Ddb.attributeValue & Ddb.avNULL ?~ True

data DynamoValue =
    DynamoBool Bool
  | DynamoByteString ByteString
  | DynamoByteStringSet (Set ByteString)
  | DynamoObject DynamoObject
  | DynamoNull
  | DynamoNumber Text1
  | DynamoNumberSet (Set Text1)
  | DynamoString Text1
  | DynamoStringSet (Set Text1)
  | DynamoList DynamoList

withBool :: String -> (Bool -> Parser a) -> DynamoValue -> Parser a
withBool expected f = \case
  DynamoBool bool -> f bool
  actual          -> typeMismatch expected actual

withByteString :: String -> (ByteString -> Parser a) -> DynamoValue -> Parser a
withByteString expected f = \case
  DynamoByteString byteString -> f byteString
  DynamoNull                  -> f mempty
  actual                      -> typeMismatch expected actual

withByteStringSet :: String -> (Set ByteString -> Parser a) -> DynamoValue -> Parser a
withByteStringSet expected f = \case
  DynamoByteStringSet byteStrings -> f byteStrings
  DynamoNull                      -> f mempty
  actual                          -> typeMismatch expected actual

withObject :: String -> (DynamoObject -> Parser a) -> DynamoValue -> Parser a
withObject expected f = \case
  DynamoObject object -> f object
  DynamoNull          -> f mempty
  actual              -> typeMismatch expected actual

withNumber :: String -> (Text1 -> Parser a) -> DynamoValue -> Parser a
withNumber expected f = \case
  DynamoNumber number -> f number
  actual              -> typeMismatch expected actual

withNumberSet :: String -> (Set Text1 -> Parser a) -> DynamoValue -> Parser a
withNumberSet expected f = \case
  DynamoNumberSet numbers -> f numbers
  DynamoNull              -> f mempty
  actual                  -> typeMismatch expected actual

withString :: String -> (Text1 -> Parser a) -> DynamoValue -> Parser a
withString expected f = \case
  DynamoString string -> f string
  actual              -> typeMismatch expected actual

withStringSet :: String -> (Set Text1 -> Parser a) -> DynamoValue -> Parser a
withStringSet expected f = \case
  DynamoStringSet strings -> f strings
  DynamoNull              -> f mempty
  actual                  -> typeMismatch expected actual

withList :: String -> (DynamoList -> Parser a) -> DynamoValue -> Parser a
withList expected f = \case
  DynamoList list        -> f list
  DynamoNull             -> f mempty
  actual                 -> typeMismatch expected actual

typeMismatch ::
  String -- ^ The name of the type you are trying to parse.
  -> DynamoValue  -- ^ The actual value encountered.
  -> Parser a
typeMismatch expected actual =
    fail $ "expected " ++ expected ++ ", encountered " ++ name
  where
    name = case actual of
      DynamoBool{}          -> "Bool"
      DynamoByteString{}    -> "ByteString"
      DynamoByteStringSet{} -> "ByteString Set"
      DynamoObject{}        -> "Object"
      DynamoNull{}          -> "Null"
      DynamoNumber{}        -> "Number"
      DynamoNumberSet{}     -> "Number Set"
      DynamoString{}        -> "String"
      DynamoStringSet{}     -> "String Set"
      DynamoList{}          -> "List"

toAV :: DynamoValue -> AttributeValue
toAV = \case
  DynamoBool a           -> Ddb.attributeValue & Ddb.avBOOL ?~ a
  DynamoByteString a     -> Ddb.attributeValue & Ddb.avB ?~ a
  DynamoByteStringSet as -> Ddb.attributeValue & Ddb.avBS .~ Set.toList as
  DynamoObject as        -> Ddb.attributeValue & Ddb.avM .~ fromText1Keys as
  DynamoNull             -> Ddb.attributeValue & Ddb.avNULL ?~ True
  DynamoNumber a         -> Ddb.attributeValue & Ddb.avN ?~ toText a
  DynamoNumberSet as     -> Ddb.attributeValue & Ddb.avNS .~ Set.toList (Set.map toText as)
  DynamoString a         -> Ddb.attributeValue & Ddb.avS ?~ toText a
  DynamoStringSet as     -> Ddb.attributeValue & Ddb.avSS .~ Set.toList (Set.map toText as)
  DynamoList as          -> Ddb.attributeValue & Ddb.avL .~ as

fromAV :: AttributeValue -> DynamoValue
fromAV av
  | Just a <- view Ddb.avBOOL av = DynamoBool a
  | Just a <- view Ddb.avB av    = DynamoByteString a
  | Just _ <- view Ddb.avNULL av = DynamoNull
  | Just a <- view Ddb.avN av    = textTo DynamoNumber a
  | Just a <- view Ddb.avS av    = textTo DynamoString a
  | as@(_:_) <- view Ddb.avBS av = DynamoByteStringSet $ Set.fromList as
  | as@(_:_) <- view Ddb.avNS av = textsTo DynamoNumberSet as
  | as@(_:_) <- view Ddb.avSS av = textsTo DynamoStringSet as
  | Just as  <- asDynamoList av  = DynamoList as
  | otherwise                    = DynamoObject $ toText1Keys $ view Ddb.avM av
  where
    textTo :: (Text1 -> DynamoValue) -> Text -> DynamoValue
    textTo f t = maybe DynamoNull f $ textToText1M t
    textsTo f as = guardeEmptySet f $ Set.fromList $ mapMaybe textToText1M as
    guardeEmptySet f set
      | Set.null set = DynamoNull
      | otherwise = f set

-- Currently it is impossible to distinguish between an empty list and an empty map by simply
-- inspecting the lenses avM and avL, both of which yield mempty even if the value is not of
-- that type. See https://github.com/brendanhay/amazonka/issues/282
asDynamoList :: AttributeValue -> Maybe DynamoList
asDynamoList av
  | Object o <- toJSON av
  , ["L"] <- toUpper <$> HashMap.keys o = Just $ view Ddb.avL av
  | otherwise = Nothing
