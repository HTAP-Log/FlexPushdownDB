//
// Created by Yifei Yang on 9/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTWEIGHTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTWEIGHTMESSAGE_H

#include <normal/core/message/Message.h>
#include <normal/cache/SegmentKey.h>
#include <unordered_map>

using namespace normal::cache;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * A message to update segment weights
 */
class WeightRequestMessage : public Message {

public:
  WeightRequestMessage(const std::unordered_map<std::shared_ptr<SegmentKey>, double>& weightMap,
                       long queryId,
                       const std::string &sender);
  WeightRequestMessage() = default;
  WeightRequestMessage(const WeightRequestMessage&) = default;
  WeightRequestMessage& operator=(const WeightRequestMessage&) = default;

  static std::shared_ptr<WeightRequestMessage> make(const std::unordered_map<std::shared_ptr<SegmentKey>, double>& weightMap,
                                                    long queryId,
                                                    const std::string &sender);

  const std::unordered_map<std::shared_ptr<SegmentKey>, double> &getWeightMap() const;
  long getQueryId() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, double> weightMap_;
  long queryId_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, WeightRequestMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type()),
                                f.field("sender", msg.sender()),
                                f.field("weightMap", msg.weightMap_),
                                f.field("queryId", msg.queryId_));
  };
};

}

using WeightRequestMessagePtr = std::shared_ptr<normal::core::cache::WeightRequestMessage>;

namespace caf {
template <>
struct inspector_access<WeightRequestMessagePtr> : variant_inspector_access<WeightRequestMessagePtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTWEIGHTMESSAGE_H
