//
// Created by Matt Youill on 31/12/19.
//

#ifndef NORMAL_OPERATORACTOR_H
#define NORMAL_OPERATORACTOR_H

#include <memory>
#include <queue>

#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include "normal/core/message/Message.h"
#include "normal/core/Operator.h"
#include "normal/core/message/StartMessage.h"
#include <normal/core/Forward.h>

using namespace ::caf;

namespace normal::core {

/**
 * Operator actor implements caf::actor and combines the operators behaviour and state
 */
class OperatorActor : public event_based_actor {

public:
  long getProcessingTime() const;
  void incrementProcessingTime(long time);
  bool running_ = false;
  std::string name_;
  std::queue<std::pair<::caf::message, strong_actor_ptr>> buffer_;
  std::optional<strong_actor_ptr> overriddenMessageSender_;

  OperatorActor(actor_config &cfg, std::shared_ptr<Operator> opBehaviour);
  std::shared_ptr<Operator> operator_() const;
  behavior make_behavior() override;
  void on_exit() override;

  const char* name() const override {
    return name_.c_str();
  }

private:
  std::shared_ptr<Operator> opBehaviour_;
  long processingTime_ = 0;
};

}

CAF_BEGIN_TYPE_ID_BLOCK(OperatorActor, normal::util::OperatorActor_first_custom_type_id)
CAF_ADD_ATOM(OperatorActor, GetProcessingTimeAtom)
CAF_END_TYPE_ID_BLOCK(OperatorActor)

#endif //NORMAL_OPERATORACTOR_H
