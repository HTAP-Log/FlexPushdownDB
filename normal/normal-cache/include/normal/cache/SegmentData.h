//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTDATA_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTDATA_H

#include <memory>
#include <normal/util/CAFUtil.h>
#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;
// FIXME: unsure why there is a ambiguous namespace lookup with gandiva::Column, as it's not included
using TupleColumn = normal::tuple::Column;

namespace normal::cache {

class SegmentData {

public:
  explicit SegmentData(std::shared_ptr<TupleColumn> column);
  SegmentData() = default;
  SegmentData(const SegmentData&) = default;
  SegmentData& operator=(const SegmentData&) = default;

  static std::shared_ptr<SegmentData> make(const std::shared_ptr<TupleColumn> &column);
  [[nodiscard]] const std::shared_ptr<TupleColumn> &getColumn() const;

private:
  std::shared_ptr<TupleColumn> column_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SegmentData& segmentData) {
    return f.apply(segmentData.column_);
  };
};

}

using SegmentDataPtr = std::shared_ptr<normal::cache::SegmentData>;

CAF_BEGIN_TYPE_ID_BLOCK(SegmentData, normal::util::SegmentData_first_custom_type_id)
CAF_ADD_TYPE_ID(SegmentData, (normal::cache::SegmentData))
CAF_END_TYPE_ID_BLOCK(SegmentData)

namespace caf {
template <>
struct inspector_access<SegmentDataPtr> : variant_inspector_access<SegmentDataPtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTDATA_H
