//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorContext.h"

#include <utility>
#include <cassert>
#include <normal/core/message/CompleteMessage.h>

#include "normal/core/Globals.h"
#include "normal/core/message/Message.h"
#include "normal/core/Actors.h"

namespace normal::core {

void OperatorContext::tell(std::shared_ptr<message::Message> &msg) {

  assert(this);

  OperatorActor* oa = this->operatorActor();
  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operator_->consumers()){
    caf::actor actorHandle = consumer.second->actorHandle();
    oa->send(actorHandle, e);
  }
}

OperatorContext::OperatorContext(std::shared_ptr<normal::core::Operator> op,  caf::actor& rootActor):
    operator_(std::move(op)),
    operatorActor_(nullptr),
    rootActor_(rootActor)
{}

std::shared_ptr<normal::core::Operator> OperatorContext::op() {
  return operator_;
}

LocalOperatorDirectory &OperatorContext::operatorMap() {
  return operatorMap_;
}
OperatorActor* OperatorContext::operatorActor() {
  return operatorActor_;
}
void OperatorContext::operatorActor(OperatorActor *operatorActor) {
  operatorActor_ = operatorActor;
}

/**
 * Sends a CompleteMessage to all consumers and the root actor
 */
void OperatorContext::notifyComplete() {

  OperatorActor* operatorActor = this->operatorActor();

  std::shared_ptr<message::Message> msg = std::make_shared<message::CompleteMessage>(operatorActor->operator_()->name());
  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operator_->consumers()){
    caf::actor actorHandle = consumer.second->actorHandle();
    operatorActor->send(actorHandle, e);
  }

  // Send message to root actor
  operatorActor->send(rootActor_, e);
}

} // namespace
