/*--------------------------------------------------------------------------------------
 * Structure modification operations
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, ConstructWithoutSMOs)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithoutSMOs;
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, !kShuffled, kRecNum);
}

TYPED_TEST(IndexFixture, ConstructWithLeafSMOs)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithLeafSMOs;
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, !kShuffled, kRecNum);
}

TYPED_TEST(IndexFixture, ConstructWithInternalSMOs)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithInternalSMOs;
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, !kShuffled, kRecNum);
}

/*--------------------------------------------------------------------------------------
 * Read operation tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, ReadWithEmptyIndexFail)
{  //
  TestFixture::VerifyRead(0, 0, kExpectFailed);
}

/*--------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, ScanWithoutKeysPerformFullScan)
{
  TestFixture::VerifyScan(std::nullopt, std::nullopt);
}

TYPED_TEST(IndexFixture, ScanWithClosedRangeIncludeLeftRightEnd)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithoutSMOs;
  TestFixture::VerifyScan(std::make_pair(0, kRangeClosed),
                          std::make_pair(kRecNum - 1, kRangeClosed));
}

TYPED_TEST(IndexFixture, ScanWithOpenedRangeExcludeLeftRightEnd)
{
  constexpr auto kRecNum = TestFixture::kRecNumWithoutSMOs;
  TestFixture::VerifyScan(std::make_pair(0, kRangeOpened),
                          std::make_pair(kRecNum - 1, kRangeOpened));
}

/*--------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, WriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, WriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, WriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, RandomWriteWithUniqueKeysSucceed)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomWriteWithDeletedKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, InsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, InsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, InsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, RandomInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, UpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, UpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, UpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, RandomUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(IndexFixture, DeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, DeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, DeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, !kShuffled);
}

TYPED_TEST(IndexFixture, RandomDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(IndexFixture, RandomDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, kShuffled);
}
