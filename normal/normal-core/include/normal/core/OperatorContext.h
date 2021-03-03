//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include <memory>
#include <string>
#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include "normal/core/Operator.h"
#include "normal/core/OperatorActor.h"
#include "normal/core/LocalOperatorDirectory.h"
#include "normal/core/message/Message.h"
#include <normal/core/Forward.h>

namespace normal::core {

/**
 * The API operators use to interact with their environment, e.g. sending messages
 */
class OperatorContext {
private:
  OperatorActor* operatorActor_;
  LocalOperatorDirectory operatorMap_;
  caf::actor rootActor_;
  caf::actor segmentCacheActor_;
  bool complete_ = false;

public:
  OperatorContext(caf::actor rootActor, caf::actor segmentCacheActor);
  OperatorContext() = default;
  OperatorContext(const OperatorContext&) = default;
  OperatorContext& operator=(const OperatorContext&) = default;

  OperatorActor* operatorActor();
  void operatorActor(OperatorActor *operatorActor);

  LocalOperatorDirectory &operatorMap();

  void tell(std::shared_ptr<message::Message> &msg);

  void notifyComplete();

  tl::expected<void, std::string> send(const std::shared_ptr<message::Message> &msg, const std::string &recipientId);

  void destroyActorHandles();
  bool isComplete() const;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, OperatorContext& ctx) {
    return f.object(ctx).fields(f.field("operatorMap", ctx.operatorMap_),
                                f.field("rootActor", ctx.rootActor_),
                                f.field("complete", ctx.complete_));
  }
};

}

using OperatorContextPtr = std::shared_ptr<normal::core::OperatorContext>;

CAF_BEGIN_TYPE_ID_BLOCK(OperatorContext, normal::util::OperatorContext_first_custom_type_id)
CAF_ADD_TYPE_ID(OperatorContext, (normal::core::OperatorContext))
CAF_END_TYPE_ID_BLOCK(OperatorContext)

namespace caf {
template <>
struct inspector_access<OperatorContextPtr> : variant_inspector_access<OperatorContextPtr> {
    // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
