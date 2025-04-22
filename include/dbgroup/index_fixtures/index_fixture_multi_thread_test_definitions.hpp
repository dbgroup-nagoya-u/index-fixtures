/*------------------------------------------------------------------------------
 * Write operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, kRandom);
}

/*------------------------------------------------------------------------------
 * Upsert operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertsWith(kWriteTwice, kWithDelete, kRandom);
}

/*------------------------------------------------------------------------------
 * Insert operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, kRandom);
}

/*------------------------------------------------------------------------------
 * Update operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, kRandom);
}

/*------------------------------------------------------------------------------
 * Delete operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, kRandom);
}

/*------------------------------------------------------------------------------
 * Concurrent Split/Merge
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, ConcurrentMixedOperationsSucceed)
{
  TestFixture::VerifyConcurrentSMOs();
}

/*------------------------------------------------------------------------------
 * Bulkload operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithoutAdditionalWriteOperations)
{
  TestFixture::VerifyBulkloadWith(kWithoutWrite, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithSequentialWrite)
{
  TestFixture::VerifyBulkloadWith(kWrite, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithSequentialInsert)
{
  TestFixture::VerifyBulkloadWith(kInsert, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithSequentialUpdate)
{
  TestFixture::VerifyBulkloadWith(kUpdate, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithSequentialDelete)
{
  TestFixture::VerifyBulkloadWith(kDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithReverseWrite)
{
  TestFixture::VerifyBulkloadWith(kWrite, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithReverseInsert)
{
  TestFixture::VerifyBulkloadWith(kInsert, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithReverseUpdate)
{
  TestFixture::VerifyBulkloadWith(kUpdate, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithReverseDelete)
{
  TestFixture::VerifyBulkloadWith(kDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithRandomWrite)
{
  TestFixture::VerifyBulkloadWith(kWrite, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithRandomInsert)
{
  TestFixture::VerifyBulkloadWith(kInsert, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithRandomUpdate)
{
  TestFixture::VerifyBulkloadWith(kUpdate, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, BulkloadWithRandomDelete)
{
  TestFixture::VerifyBulkloadWith(kDelete, kRandom);
}
