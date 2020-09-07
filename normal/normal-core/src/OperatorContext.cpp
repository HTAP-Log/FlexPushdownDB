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
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/core/cache/SegmentCacheActor2.h>

using namespace normal::core::cache;

namespace normal::core {

void OperatorContext::tell(std::shared_ptr<message::Message> &msg) {

  assert(this);

  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operator_->consumers()){
    caf::actor actorHandle = consumer.second.lock()->actorHandle();
    operatorActor_->send(actorHandle, e);
  }
}

//void OperatorContext::tell_pushDownMode(std::shared_ptr<message::Message> &msg) {
//    assert(this);
//
//    OperatorActor* oa = this->operatorActor();
//    message::Envelope e(msg);
//
//    // Send message to filter consumers
//    for(const auto& consumer: this->operator_->consumers()){
//        if (consumer.second->getType()!="Filter") {
//            caf::actor actorHandle = consumer.second->actorHandle();
//            oa->send(actorHandle, e);
//        }
//    }
//}
//    void OperatorContext::tell_pullUpMode(std::shared_ptr<message::Message> &msg) {
//        assert(this);
//
//        OperatorActor* oa = this->operatorActor();
//        message::Envelope e(msg);
//
//        // Send message to filter consumers
//        for(const auto& consumer: this->operator_->consumers()){
//            if (consumer.second->getType()=="Filter") {
//                caf::actor actorHandle = consumer.second->actorHandle();
//                oa->send(actorHandle, e);
//            }
//        }
//    }

tl::expected<void, std::string> OperatorContext::send(const std::shared_ptr<message::Message> &msg, const std::string& recipientId) {

  if(recipientId == "SegmentCache"){

	auto segmentCacheActor = operatorMap_.get("SegmentCache").value().getActor().value();

	if(msg->type() == "LoadRequestMessage"){

	  auto loadMessage = std::static_pointer_cast<LoadRequestMessage>(msg);

	  this->operatorActor_->request(segmentCacheActor, ::caf::infinite, LoadAtom::value, loadMessage->getSegmentKeys()).then(
		  [=](const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>& segments,
			  const std::vector<std::shared_ptr<SegmentKey>>& segmentKeysToCache) {
			Envelope e{LoadResponseMessage::make(segments, loadMessage->sender(), segmentKeysToCache)};
			this->operatorActor_->send(this->operatorActor_, e);
		  }
	  );

	  return {};
	}
	else if(msg->type() == "StoreRequestMessage"){
	  auto storeMessage = std::static_pointer_cast<StoreRequestMessage>(msg);
	  this->operatorActor_->send(segmentCacheActor, StoreAtom::value, storeMessage->getSegments());
	  return {};
	}
	else	{
	  return tl::unexpected(fmt::format("Unrecognized message type {}", msg->type()));
	}
  }
  else {
	message::Envelope e(msg);

	auto expectedOperator = operatorMap_.get(recipientId);
	if (expectedOperator.has_value()) {
	  auto recipientOperator = expectedOperator.value();
	  auto expectedRecipientActor = recipientOperator.getActor();
	  auto recipientActor = expectedRecipientActor.value();
	  operatorActor_->send(recipientActor, e);
	  return {};
	} else {
	  return tl::unexpected(fmt::format("Actor with id '{}' not found", recipientId));
	}
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

  SPDLOG_DEBUG("Completing operator  |  name: '{}'", this->operator_->name());

  OperatorActor* operatorActor = this->operatorActor();

  std::shared_ptr<message::Message> msg = std::make_shared<message::CompleteMessage>(operatorActor->operator_()->name());
  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operator_->consumers()){
    caf::actor actorHandle = consumer.second.lock()->actorHandle();
    operatorActor->send(actorHandle, e);
  }

  // Send message to root actor
  operatorActor->send(rootActor_, e);

  operatorActor->quit();
}
void OperatorContext::setRootActor(const caf::actor &RootActor) {
  rootActor_ = RootActor;
}

} // namespace
