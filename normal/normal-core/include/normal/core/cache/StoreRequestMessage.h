//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H

#include <normal/core/message/Message.h>

#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>

using namespace normal::cache;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * Reuest for the segment cache actor to store the given segment data given a segment key.
 */
class StoreRequestMessage : public Message {

public:
  StoreRequestMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
					  const std::string &sender);
  StoreRequestMessage() = default;
  StoreRequestMessage(const StoreRequestMessage&) = default;
  StoreRequestMessage& operator=(const StoreRequestMessage&) = default;

  static std::shared_ptr<StoreRequestMessage>
  make(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
	   const std::string &sender);

  [[nodiscard]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> &
  getSegments() const;

  [[nodiscard]] std::string toString() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StoreRequestMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type()),
                                f.field("sender", msg.sender()),
                                f.field("segments", msg.segments_));
  };
};

}

using StoreRequestMessagePtr = std::shared_ptr<normal::core::cache::StoreRequestMessage>;

namespace caf {
template <>
struct inspector_access<StoreRequestMessagePtr> : variant_inspector_access<StoreRequestMessagePtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H
