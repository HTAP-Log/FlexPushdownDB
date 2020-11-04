//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/core/OperatorManager.h>
#include <normal/pushdown/shuffle/ATTIC/Shuffler.h>
#include <normal/pushdown/shuffle/ShuffleKernel2.h>
#include <normal/pushdown/shuffle/ShuffleKernel3.h>
#include <normal/pushdown/shuffle/ATTIC/ShuffleKernel.h>

using namespace normal::tuple;
using namespace normal::pushdown;
using namespace normal::pushdown::shuffle;

namespace {

//void run1(const std::shared_ptr<TupleSet2> &tupleSet) {
//
//  auto expectedShuffledTupleSets = Shuffler::shuffle("a", 2, tupleSet);
//	  REQUIRE(expectedShuffledTupleSets.has_value());
//  auto shuffledTupleSets = expectedShuffledTupleSets.value();
//
//  size_t partitionIndex = 0;
//  for (const auto &shuffledTupleSet: shuffledTupleSets) {
//	SPDLOG_DEBUG("Output: partitionIndex: {}, \n{}", partitionIndex,
//				 shuffledTupleSet->showString(TupleSetShowOptions(RowOriented)));
//	++partitionIndex;
//  }
//
//}

//void run2(const std::shared_ptr<TupleSet2> &tupleSet) {
//
//  auto expectedShuffledTupleSets = ShuffleKernel::shuffle("a", 2, tupleSet);
//	  REQUIRE(expectedShuffledTupleSets.has_value());
//  auto shuffledTupleSets = expectedShuffledTupleSets.value();
//
//  size_t partitionIndex = 0;
//  for (const auto &shuffledTupleSet: shuffledTupleSets) {
//	SPDLOG_DEBUG("Output: partitionIndex: {}, \n{}", partitionIndex,
//				 shuffledTupleSet->showString(TupleSetShowOptions(RowOriented)));
//	++partitionIndex;
//  }
//
//}

void run3(const std::shared_ptr<TupleSet2> &tupleSet) {

  auto expectedShuffledTupleSets = ShuffleKernel2::shuffle("a", 2, *tupleSet);
	  REQUIRE(expectedShuffledTupleSets.has_value());
  auto shuffledTupleSets = expectedShuffledTupleSets.value();

  size_t partitionIndex = 0;
  for (const auto &shuffledTupleSet: shuffledTupleSets) {
	SPDLOG_DEBUG("Output: partitionIndex: {}, \n{}", partitionIndex,
				 shuffledTupleSet->showString(TupleSetShowOptions(RowOriented)));
	++partitionIndex;
  }

}

void run4(const std::shared_ptr<TupleSet2> &tupleSet) {

  ShuffleKernel3 kernel("a", 2);

  auto expectedShuffledTupleSets = kernel.shuffle(*tupleSet);
	  REQUIRE(expectedShuffledTupleSets.has_value());
  const auto &shuffledTupleSets = expectedShuffledTupleSets.value();

  size_t partitionIndex = 0;
  for (const auto &shuffledTupleSet: shuffledTupleSets) {
	SPDLOG_DEBUG("Output 1: partitionIndex: {}, \n{}", partitionIndex,
				 shuffledTupleSet->showString(TupleSetShowOptions(RowOriented)));
	++partitionIndex;
  }

  auto expectedFinalisedShuffledTupleSets = kernel.toTupleSets(true);
	  REQUIRE(expectedFinalisedShuffledTupleSets.has_value());
  const auto &finalisedShuffledTupleSets = expectedFinalisedShuffledTupleSets.value();

  partitionIndex = 0;
  for (const auto &finalisedShuffledTupleSet: finalisedShuffledTupleSets) {
	SPDLOG_DEBUG("Output 2: partitionIndex: {}, \n{}", partitionIndex,
				 finalisedShuffledTupleSet->showString(TupleSetShowOptions(RowOriented)));
	++partitionIndex;
  }

}

}

TEST_SUITE ("shuffle" * doctest::skip(false)) {

TEST_CASE ("shuffle-basic" * doctest::skip(false)) {

  auto stringType = arrow::utf8();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto columnA = std::vector{"1", "2", "3"};
  auto columnB = std::vector{"4", "5", "6"};
  auto columnC = std::vector{"7", "8", "9"};

  auto arrowColumnA = Arrays::make<arrow::StringType>(columnA).value();
  auto arrowColumnB = Arrays::make<arrow::StringType>(columnB).value();
  auto arrowColumnC = Arrays::make<arrow::StringType>(columnC).value();

  auto tupleSet = TupleSet2::make(schema, {arrowColumnA, arrowColumnB, arrowColumnC});

  SPDLOG_DEBUG("Input:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

//  run1(tupleSet);
//  run2(tupleSet);
  run3(tupleSet);
  run4(tupleSet);

}

TEST_CASE ("shuffle-empty" * doctest::skip(false)) {

  auto stringType = arrow::utf8();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto columnA = std::vector<std::string>{};
  auto columnB = std::vector<std::string>{};
  auto columnC = std::vector<std::string>{};

  auto arrowColumnA = Arrays::make<arrow::StringType>(columnA).value();
  auto arrowColumnB = Arrays::make<arrow::StringType>(columnB).value();
  auto arrowColumnC = Arrays::make<arrow::StringType>(columnC).value();

  auto tupleSet = TupleSet2::make(schema, {arrowColumnA, arrowColumnB, arrowColumnC});

  SPDLOG_DEBUG("Input:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

//  run1(tupleSet);
//  run2(tupleSet);
  run3(tupleSet);
  run4(tupleSet);
}

}