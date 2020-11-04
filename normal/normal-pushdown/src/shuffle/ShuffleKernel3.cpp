//
// Created by matt on 30/10/20.
//

#include "normal/pushdown/shuffle/ShuffleKernel3.h"

#include <string>

#include <arrow/api.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/ArrayHasher.h>
#include <normal/pushdown/Globals.h>

using namespace normal::pushdown::shuffle;

ShuffleKernel3::ShuffleKernel3(std::string shuffleColumnName,
							   size_t numSlots) :
	shuffleColumnName_(std::move(shuffleColumnName)),
	numSlots_(numSlots),
	shuffledAppendersVector_(numSlots) {
}

tl::expected<void, std::string> ShuffleKernel3::cache(const TupleSet2 &tupleSet) {

  if (!cached_) {
	// Get the shuffle column index, checking the column exists
	shuffleColumnIndex_ =
		tupleSet.getArrowTable().value()->schema()->GetFieldIndex(ColumnName::canonicalize(shuffleColumnName_));
	if (shuffleColumnIndex_ == -1)
	  return tl::make_unexpected(fmt::format("Shuffle column '{}' does not exist", shuffleColumnName_));

	// Cache schema
	schema_ = tupleSet.getArrowTable().value()->schema();

	// Create appenders
	for (size_t s = 0; s < numSlots_; ++s) {
	  shuffledAppendersVector_[s] =
		  std::vector<std::shared_ptr<ArrayAppender>>(static_cast<size_t>(schema_.value()->num_fields()));

	  const auto &makeAppenderResult = makeAppenders(s);
	  if (!makeAppenderResult)
		return tl::make_unexpected(makeAppenderResult.error());
	}

	cached_ = true;

  } else {
	if (!schema_.value()->Equals(tupleSet.getArrowTable().value()->schema())) {
	  // Check schemas equal
	  return tl::make_unexpected("TupleSet schemas must be equal");
	}
  }

  return {};
}

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> ShuffleKernel3::shuffle(const TupleSet2 &tupleSet) {

  const auto &cacheResult = cache(tupleSet);
  if (!cacheResult)
	return tl::make_unexpected(cacheResult.error());

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Get the arrow table, checking the tupleset is defined FIXME: This is dumb :(
  if (!tupleSet.getArrowTable().has_value())
	return tl::make_unexpected(fmt::format("TupleSet is undefined"));
  const auto &table = tupleSet.getArrowTable().value();

  // Read the table a batch at a time
  ::arrow::TableBatchReader reader{*table};
  reader.set_chunksize(DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok())
	return tl::make_unexpected(recordBatchResult.status().message());
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {

	// Shuffle batch
	const auto &expectedResult = shuffleRecordBatch(*recordBatch);
//	SPDLOG_INFO("Shuffled {} tuples, column: {}, bytesize: {}", recordBatch->num_rows(), columnName, size);
	if (!expectedResult.has_value())
	  return tl::make_unexpected(expectedResult.error());

	// Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok())
	  return tl::make_unexpected(recordBatchResult.status().message());
	recordBatch = *recordBatchResult;
  }

  auto expectedShuffledTupleSets = toTupleSets(false);

//#ifndef NDEBUG
//
//  assert(expectedShuffledTupleSets.has_value());
//
//  size_t totalNumRows = 0;
//  for (auto shuffledTupleSet: expectedShuffledTupleSets.value()) {
//	assert(shuffledTupleSet->getArrowTable().has_value());
//	assert(shuffledTupleSet->getArrowTable().value()->ValidateFull().ok());
//	totalNumRows += shuffledTupleSet->numRows();
//  }
//
//  assert(totalNumRows == static_cast<size_t>(tupleSet.numRows()));
//
//#endif

  return expectedShuffledTupleSets;
}

tl::expected<arrow::ArrayVector, std::string> ShuffleKernel3::finaliseAppenders(int64_t s, bool force) {

  arrow::ArrayVector arrays(schema_.value()->num_fields());

  if (force || shuffledAppendersVector_[s].size() > DefaultBufferSize) {

	for (int c = 0; c < schema_.value()->num_fields(); ++c) {
	  const auto &expectedArray = shuffledAppendersVector_[s][c]->finalize();
	  if (!expectedArray.has_value())
		return tl::make_unexpected(expectedArray.error());
	  if (expectedArray.value()->length() > 0) {
		arrays[c] = expectedArray.value();
	  }
	}

	const auto &makeAppenderResult = makeAppenders(s);
	if (!makeAppenderResult)
	  return tl::make_unexpected(makeAppenderResult.error());
  }

  return arrays;
}

tl::expected<std::vector<arrow::ArrayVector>, std::string> ShuffleKernel3::finaliseAppenders(bool force) {

  std::vector<arrow::ArrayVector> arrayVectors(numSlots_);

  for (size_t s = 0; s < numSlots_; ++s) {
	const auto &expectedArrayVector = finaliseAppenders(s, force);
	if (!expectedArrayVector)
	  return tl::make_unexpected(expectedArrayVector.error());
	arrayVectors[s] = expectedArrayVector.value();
  }

  return arrayVectors;
}

tl::expected<void, std::string> ShuffleKernel3::makeAppenders(int64_t s) {
  for (int c = 0; c < schema_.value()->num_fields(); ++c) {
	const auto &expectedAppender = ArrayAppenderBuilder::make(schema_.value()->field(c)->type(), DefaultBufferSize * 2);
	if (!expectedAppender.has_value())
	  return tl::make_unexpected(expectedAppender.error());

	shuffledAppendersVector_[s][c] = expectedAppender.value();
  }
  return {};
}

tl::expected<void, std::string>
ShuffleKernel3::shuffleRecordBatch(const ::arrow::RecordBatch &recordBatch) {

  // Get a reference to the array to shuffle on
  const auto &shuffleColumn = recordBatch.column(shuffleColumnIndex_.value());

  // Create a hasher for the shuffle array
  const auto &expectedShuffleColumnHasher = ArrayHasher::make(shuffleColumn);
  if (!expectedShuffleColumnHasher)
	return tl::make_unexpected(expectedShuffleColumnHasher.error());
  const auto &shuffleColumnHasher = expectedShuffleColumnHasher.value();

  // Pre compute slots
  std::vector<size_t> slots(shuffleColumn->length());
  for (int64_t r = 0; r < shuffleColumn->length(); ++r) {
	slots[r] = shuffleColumnHasher->hash(r) % numSlots_;
  }

  // Assign values to slots
  for (int c = 0; c < recordBatch.num_columns(); ++c) {
	for (int64_t r = 0; r < shuffleColumn->length(); ++r) {
	  shuffledAppendersVector_[slots[r]][c]->appendValue(recordBatch.column(c), r);
	}
  }

  return {};
}

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> ShuffleKernel3::toTupleSets(bool force) {

  const auto &expectedArrayVectors = finaliseAppenders(force);
  if (!expectedArrayVectors)
	return tl::make_unexpected(expectedArrayVectors.error());
  const auto &arrayVectors = expectedArrayVectors.value();

  // Create TupleSets from the destination vectors of arrays
  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSetVector(numSlots_);
  for (size_t s = 0; s < arrayVectors.size(); ++s) {
	// check empty
	if (!arrayVectors[s][0]) {
	  shuffledTupleSetVector[s] = TupleSet2::make(Schema::make(schema_.value()));
	} else {
	  std::shared_ptr<::arrow::Table> shuffledTable = ::arrow::Table::Make(schema_.value(), arrayVectors[s]);
	  shuffledTupleSetVector[s] = TupleSet2::make(shuffledTable);
	}
  }

  return shuffledTupleSetVector;
}
