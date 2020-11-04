//
// Created by matt on 30/10/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL3_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL3_H

#include <vector>
#include <memory>
#include <string>

#include <tl/expected.hpp>

#include <normal/tuple/TupleSet2.h>
#include <normal/tuple/ArrayAppender.h>

using namespace normal::tuple;

namespace normal::pushdown::shuffle {

/**
 * Shuffles a given tuple set into numSlots tuplesets based on the values in the column with the given name.
 */
class ShuffleKernel3 {

public:
  ShuffleKernel3(std::string shuffleColumnName, size_t numSlots);

  [[nodiscard]] tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> shuffle(const TupleSet2& tupleSet);
  [[nodiscard]] tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> toTupleSets(bool force);

private:

  std::string shuffleColumnName_;
  size_t numSlots_;

  bool cached_ = false;
  std::optional<int> shuffleColumnIndex_;
  std::optional<std::shared_ptr<::arrow::Schema>> schema_;
  std::vector<std::vector<std::shared_ptr<ArrayAppender>>> shuffledAppendersVector_;

  [[nodiscard]] tl::expected<void, std::string> cache(const TupleSet2& tupleSet);
  [[nodiscard]] tl::expected<void, std::string> shuffleRecordBatch(const ::arrow::RecordBatch &recordBatch);
  [[nodiscard]] tl::expected<void, std::string> makeAppenders(int64_t s);
  [[nodiscard]] tl::expected<arrow::ArrayVector, std::string> finaliseAppenders(int64_t s, bool force);
  [[nodiscard]] tl::expected<std::vector<arrow::ArrayVector>, std::string> finaliseAppenders(bool force);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL3_H
