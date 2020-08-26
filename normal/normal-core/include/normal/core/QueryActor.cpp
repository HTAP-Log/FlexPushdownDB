//
// Created by matt on 24/8/20.
//

#include "QueryActor.h"

#include <caf/all.hpp>

using namespace normal::core;

QueryActor::behavior_type normal::core::queryBehaviour(QueryActor::stateful_pointer <QueryActorState> self, std::string name) {

  self->state.name = std::move(name);

  setDefaultHandlers(*self);

  self->set_exit_handler([&](const ::caf::exit_msg &m) {
	SPDLOG_DEBUG("Actor Exit  |  receiver: {} ('{}'), exit sender: {}, exit reason: {}", self->id(),
				 self->name(), to_string(m.source), to_string(m.reason));
	self->quit(m.reason);
  });

  self->set_down_handler([=](const ::caf::down_msg &m) {
	SPDLOG_WARN("Actor Down  |  receiver: {} ('{}'), down actor: {}, down reason: {}", self->id(),
				self->name(), to_string(m.source), to_string(m.reason));

	// If the shutdown wasn't normal, shutdown the entire query
	if(m.reason != ::caf::exit_reason::normal){
	  for(const auto& op: self->state.g.lock()->getOperators()){
		if(op.second->operatorActor()->alive())
			self->send_exit(op.second->operatorActor(), m.reason);
	  }
	  self->quit(m.reason);
	}
  });

  return {
	  [=](ExecuteAtom, const std::shared_ptr<OperatorGraph>& g) {
		SPDLOG_DEBUG("Query Execute  |  receiver: {} ('{}'), sender: {}", self->id(), self->name(), to_string(self->current_sender()));

		self->state.sender = self->current_sender();
		self->state.g = g;

		boot2(*self->state.g.lock(), self);

		for(const auto& op: self->state.g.lock()->getOperators()){
		  self->monitor(op.second->operatorActor());
		}

		start(*self->state.g.lock(), self);
	  },
	  [=](const Envelope& e) {

		auto m = e.getMessage();
	    auto &mRef = *m;

		if(typeid(mRef) == typeid(CompleteMessage)){

		  SPDLOG_DEBUG("Operator Complete  |  receiver: {} ('{}'), sender: {}", self->id(), self->name(), to_string(self->current_sender()));

		  auto completeMessage = std::static_pointer_cast<CompleteMessage>(m);

		  self->state.g.lock()->getOperatorDirectory().setComplete(completeMessage->sender());

		  if(self->state.g.lock()->getOperatorDirectory().allComplete()){

			auto tupleSet = TupleSet2::make();
			self->anon_send(::caf::actor_cast<actor>(self->state.sender), tupleSet);

			self->quit(exit_reason::normal);
		  }
		}
		else{
		  auto error = ::caf::make_error(::caf::sec::unexpected_message, fmt::format("Envelope contained unexpected message of type '{}'", typeid(mRef).name()));
		  self->call_error_handler(error);
		}
	  }
  };
}

void normal::core::boot2(OperatorGraph &g, const QueryActor::stateful_pointer <QueryActorState> self) {

  // Create the operators
  for (const auto &element: g.getOperators()) {
	auto ctx = element.second;
	ctx->setRootActor(actor_cast<actor>(self));
	auto op = ctx->op();
	op->create(ctx);
  }

  // Create the operator actors
  for (const auto &element: g.getOperators()) {
	auto ctx = element.second;
	auto op = ctx->op();
	caf::actor actorHandle = self->spawn<normal::core::OperatorActor>(op);
	op->actorHandle(actorHandle);
  }

  // Tell the actors about the system actors
  for (const auto &element:  g.getOperators()) {

	auto ctx = element.second;
	auto op = ctx->op();

	auto rootActorEntry = LocalOperatorDirectoryEntry(self->name(),
													  std::optional(::caf::actor_cast<actor>(self)),
													  OperatorRelationshipType::None,
													  false);

	ctx->operatorMap().insert(rootActorEntry);

//	auto segmentCacheActorEntry = LocalOperatorDirectoryEntry(operatorManager_.lock()->getSegmentCacheActor()->name(),
//															  std::optional(operatorManager_.lock()->getSegmentCacheActor()->actorHandle()),
//															  OperatorRelationshipType::None,
//															  false);
//
//	ctx->operatorMap().insert(segmentCacheActorEntry);
  }
//
//  // Tell the system actors about the other actors
//  for (const auto &element: m_operatorMap) {
//
//	auto ctx = element.second;
//	auto op = ctx->op();
//
//	auto entry = LocalOperatorDirectoryEntry(op->name(),
//											 op->actorHandle(),
//											 OperatorRelationshipType::None,
//											 false);
//
//	operatorManager_.lock()->getSegmentCacheActor()->ctx()->operatorMap().insert(entry);
//  }

  // Tell the actors who their producers are
  for (const auto &element:  g.getOperators()) {
	auto ctx = element.second;
	auto op = ctx->op();
	for (const auto &producerEntry: op->producers()) {
	  auto producer = producerEntry.second;
	  auto entry = LocalOperatorDirectoryEntry(producer.lock()->name(),
											   producer.lock()->actorHandle(),
											   OperatorRelationshipType::Producer,
											   false);
	  ctx->operatorMap().insert(entry);
	}
  }

  // Tell the actors who their consumers are
  for (const auto &element:  g.getOperators()) {
	auto ctx = element.second;
	auto op = ctx->op();
	for (const auto &consumerEntry: op->consumers()) {
	  auto consumer = consumerEntry.second;
	  auto entry = LocalOperatorDirectoryEntry(consumer.lock()->name(),
											   consumer.lock()->actorHandle(),
											   OperatorRelationshipType::Consumer,
											   false);
	  ctx->operatorMap().insert(entry);
	}
  }
}

void normal::core::start(OperatorGraph &g, const QueryActor::stateful_pointer <QueryActorState> self) {

  self->state.startTime = std::chrono::steady_clock::now();

  // Mark all the operators as incomplete
  g.getOperatorDirectory().setIncomplete();


//   Send start messages to the actors
  for (const auto &element: g.getOperators()) {
	auto ctx = element.second;
	auto op = ctx->op();

	std::vector<caf::actor> actorHandles;
	for (const auto &consumer: op->consumers())
	  actorHandles.emplace_back(consumer.second.lock()->actorHandle());

	auto sm = std::make_shared<message::StartMessage>(actorHandles, self->name());

	self->anon_send(op->actorHandle(), normal::core::message::Envelope(sm));
  }
}
