/*--------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, WriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, WriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, WriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, InsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, InsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, InsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, UpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, UpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, UpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, DeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, DeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, DeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, kShuffled);
}
