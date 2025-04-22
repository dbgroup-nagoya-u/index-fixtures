/*----------------------------------------------------------------------------*
 * Write operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Upsert operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Insert operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertWith(kWriteTwice, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Update operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdateWith(kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdateWith(!kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdateWith(kWithWrite, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdateWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdateWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdateWith(kWithWrite, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdateWith(kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdateWith(!kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdateWith(kWithWrite, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Delete operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, SequentialDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeleteWith(kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeleteWith(!kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, SequentialDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeleteWith(kWithWrite, kWithDelete, kSequential);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeleteWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeleteWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, ReverseDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeleteWith(kWithWrite, kWithDelete, kReverse);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeleteWith(kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeleteWith(!kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexMultiThreadFixture, RandomDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeleteWith(kWithWrite, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Concurrent Split/Merge
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexMultiThreadFixture, ConcurrentMixedOperationsSucceed)
{
  TestFixture::VerifyConcurrentSMOs();
}

/*----------------------------------------------------------------------------*
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
