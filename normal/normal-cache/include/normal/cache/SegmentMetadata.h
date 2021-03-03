//
// Created by Yifei Yang on 7/30/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H

#include <memory>
#include <normal/util/CAFUtil.h>

namespace normal::cache {

class SegmentMetadata {

public:
  SegmentMetadata();
  SegmentMetadata(size_t estimateSize, size_t size);
  SegmentMetadata(const SegmentMetadata &m);
  SegmentMetadata& operator=(const SegmentMetadata&) = default;

  static std::shared_ptr<SegmentMetadata> make();
  static std::shared_ptr<SegmentMetadata> make(size_t estimateSize, size_t size);

  void setSize(size_t size);
  size_t size() const;
  int hitNum() const;
  size_t estimateSize() const;
  double perSizeFreq() const;

  double value() const;
  double avgValue() const;
  double value2() const;
  bool valid() const;

  void incHitNum();
  void incHitNum(size_t size);
  void addValue(double value);
  void invalidate();

private:
  size_t estimateSize_;
  size_t size_;
  int hitNum_;
  double perSizeFreq_;
  double value_;
  bool valid_;

// caf inspect
public:
template <class Inspector>
friend bool inspect(Inspector& f, SegmentMetadata& m) {
  return f.object(m).fields(f.field("estimateSize", m.estimateSize_),
                            f.field("size", m.size_),
                            f.field("hitNum", m.hitNum_),
                            f.field("perSizeFreq", m.perSizeFreq_),
                            f.field("value", m.value_),
                            f.field("valid", m.valid_));
}
};

}

CAF_BEGIN_TYPE_ID_BLOCK(SegmentMetadata, normal::util::SegmentMetadata_first_custom_type_id)
CAF_ADD_TYPE_ID(SegmentMetadata, (normal::cache::SegmentMetadata))
CAF_END_TYPE_ID_BLOCK(SegmentMetadata)

using SegmentMetadataPtr = std::shared_ptr<normal::cache::SegmentMetadata>;

namespace caf {
template <>
struct inspector_access<SegmentMetadataPtr> : variant_inspector_access<SegmentMetadataPtr> {
    // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
