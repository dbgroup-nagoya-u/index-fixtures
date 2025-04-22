/*----------------------------------------------------------------------------*
 * Structure modification operations
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, ConstructWithoutSMOs)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithoutSMOs;
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kSequential, kRecNum);
}

TYPED_TEST(IndexFixture, ConstructWithLeafSMOs)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithLeafSMOs;
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kSequential, kRecNum);
}

TYPED_TEST(IndexFixture, ConstructWithInternalSMOs)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithInternalSMOs;
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kSequential, kRecNum);
}

/*----------------------------------------------------------------------------*
 * Read operation tests
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, ReadWithEmptyIndexFail)
{  //
  TestFixture::VerifyReadEmptyIndex();
}

/*----------------------------------------------------------------------------*
 * Scan operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, ScanWithClosedRangeIncludeLeftRightEnd)
{
  TestFixture::VerifyScanWith(kClosed);
}

TYPED_TEST(IndexFixture, ScanWithOpenedRangeExcludeLeftRightEnd)
{
  TestFixture::VerifyScanWith(kOpen);
}

/*----------------------------------------------------------------------------*
 * Write operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, SequentialWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, ReverseWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, RandomWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWriteWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWriteWith(kWriteTwice, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Upsert operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, SequentialUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, ReverseUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, RandomUpsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyUpsertWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomUpsertWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomUpsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyUpsertWith(kWriteTwice, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Insert operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, SequentialInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertWith(!kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertWith(kWriteTwice, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertWith(kWriteTwice, kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, ReverseInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertWith(!kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertWith(kWriteTwice, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertWith(kWriteTwice, kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, RandomInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertWith(!kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertWith(kWriteTwice, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertWith(kWriteTwice, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Update operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, SequentialUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdateWith(kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdateWith(!kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdateWith(kWithWrite, kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, ReverseUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdateWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdateWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdateWith(kWithWrite, kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, RandomUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdateWith(kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdateWith(!kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdateWith(kWithWrite, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Delete operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, SequentialDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeleteWith(kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeleteWith(!kWithWrite, !kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, SequentialDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeleteWith(kWithWrite, kWithDelete, kSequential);
}

TYPED_TEST(IndexFixture, ReverseDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeleteWith(kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeleteWith(!kWithWrite, !kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, ReverseDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeleteWith(kWithWrite, kWithDelete, kReverse);
}

TYPED_TEST(IndexFixture, RandomDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeleteWith(kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeleteWith(!kWithWrite, !kWithDelete, kRandom);
}

TYPED_TEST(IndexFixture, RandomDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeleteWith(kWithWrite, kWithDelete, kRandom);
}

/*----------------------------------------------------------------------------*
 * Bulkload operation
 *----------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, BulkloadWithoutAdditionalWriteOperations)
{
  TestFixture::VerifyBulkloadWith(kWithoutWrite, kSequential);
}

TYPED_TEST(IndexFixture, BulkloadWithSequentialWrite)
{
  TestFixture::VerifyBulkloadWith(kWrite, kSequential);
}

TYPED_TEST(IndexFixture, BulkloadWithSequentialInsert)
{
  TestFixture::VerifyBulkloadWith(kInsert, kSequential);
}

TYPED_TEST(IndexFixture, BulkloadWithSequentialUpdate)
{
  TestFixture::VerifyBulkloadWith(kUpdate, kSequential);
}

TYPED_TEST(IndexFixture, BulkloadWithSequentialDelete)
{
  TestFixture::VerifyBulkloadWith(kDelete, kSequential);
}

TYPED_TEST(IndexFixture, BulkloadWithReverseWrite)
{
  TestFixture::VerifyBulkloadWith(kWrite, kReverse);
}

TYPED_TEST(IndexFixture, BulkloadWithReverseInsert)
{
  TestFixture::VerifyBulkloadWith(kInsert, kReverse);
}

TYPED_TEST(IndexFixture, BulkloadWithReverseUpdate)
{
  TestFixture::VerifyBulkloadWith(kUpdate, kReverse);
}

TYPED_TEST(IndexFixture, BulkloadWithReverseDelete)
{
  TestFixture::VerifyBulkloadWith(kDelete, kReverse);
}

TYPED_TEST(IndexFixture, BulkloadWithRandomWrite)
{
  TestFixture::VerifyBulkloadWith(kWrite, kRandom);
}

TYPED_TEST(IndexFixture, BulkloadWithRandomInsert)
{
  TestFixture::VerifyBulkloadWith(kInsert, kRandom);
}

TYPED_TEST(IndexFixture, BulkloadWithRandomUpdate)
{
  TestFixture::VerifyBulkloadWith(kUpdate, kRandom);
}

TYPED_TEST(IndexFixture, BulkloadWithRandomDelete)
{
  TestFixture::VerifyBulkloadWith(kDelete, kRandom);
}
