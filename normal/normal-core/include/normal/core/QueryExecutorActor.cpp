//
// Created by matt on 24/8/20.
//

#include "QueryExecutorActor.h"

#include <caf/all.hpp>

#include <normal/core/Actors.h>
#include <normal/core/message/CompleteMessage.h>

using namespace normal::core;
using namespace normal::core::message;

namespace {

void onActorExit(QueryExecutorActorType self, const exit_msg &m);
result<tl::expected<std::shared_ptr<TupleSet2>, std::string>> onExecute(QueryExecutorActorType self,
																		const std::shared_ptr<OperatorGraph> &operatorGraph);
void onComplete(QueryExecutorActorType self, const Envelope &e);

[[nodiscard]] tl::expected<void, std::string> makePromise(QueryExecutorActorType self);
void resolvePromise(QueryExecutorActorType self, const std::shared_ptr<TupleSet2> &tupleSet);
void rejectPromise(QueryExecutorActorType self, const std::string &message);

void boot2(OperatorGraph &g, QueryExecutorActorType self);
void start(OperatorGraph &g, QueryExecutorActorType self);

void onActorExit(const QueryExecutorActorType self, const exit_msg &m) {

  // If the exit wasn't normal, shutdown the entire query
  if (m.reason != exit_reason::normal) {

	/*
	 * NOTE: CAF does not capture the text if the exit was because of an exception.
	 * See: https://github.com/actor-framework/actor-framework/issues/1117
	 */

	SPDLOG_ERROR("[Actor {} ('{}')]  Abnormal Operator Exit  |  source: {}, reason: {}", self->id(),
				 self->name(), to_string(m.source), to_string(m.reason));

	for (const auto &entry: self->state.operatorDirectory.getEntries()) {
	  if (entry.second.getActor().value().id() != m.source.id()) {
		self->send_exit(entry.second.getActor().value(), m.reason);
	  }
	}

	/*
	 * The actor quitting with an error doesn't terminate the requesting actor, so need to explicitly deliver an
	 * error here.
	 * See bug: https://github.com/actor-framework/actor-framework/issues/1047
	 */
	rejectPromise(self, fmt::format("Query execution error  |  reason: '{}'", to_string(m.reason)));
	self->quit(m.reason);
  } else {
	SPDLOG_DEBUG("[Actor {} ('{}')]  Normal Operator Exit  |  source: {}, reason: {}", self->id(),
				 self->name(), to_string(m.source), to_string(m.reason));
  }
}

result<tl::expected<std::shared_ptr<TupleSet2>, std::string>> onExecute(QueryExecutorActorType self,
																		const std::shared_ptr<OperatorGraph> &operatorGraph) {
  SPDLOG_DEBUG("[Actor {} ('{}')]  Query Execute  |  source: {}",
			   self->id(),
			   self->name(),
			   to_string(self->current_sender()));

  self->state.startTime = std::chrono::steady_clock::now();
  self->state.sender = self->current_sender();
  self->state.operatorGraph = operatorGraph;

  auto expectedPromise = makePromise(self);
  if (!expectedPromise) {
	tl::expected<std::shared_ptr<TupleSet2>, std::string>
		expected{tl::make_unexpected(fmt::format("Query execution error  |  reason: '{}'", expectedPromise.error()))};
	return self->response(expected);
  }

  boot2(*self->state.operatorGraph.lock(), self);

  // Link to the operator actor so any exits are captured by all linked actors
  for (const auto &op: self->state.operatorGraph.lock()->getOperators()) {
	self->link_to(op.second->operatorActor());
  }

  start(*self->state.operatorGraph.lock(), self);

  return self->state.promise;
}

tl::expected<void, std::string> makePromise(const QueryExecutorActorType self) {
  if (self->state.promise.pending()) {
	return tl::make_unexpected("Cannot make promise  |  Promise is already pending");
  }

  self->state.promise = self->make_response_promise<tl::expected<std::shared_ptr<TupleSet2>, std::string>>();
  return {};
}

void onComplete(const QueryExecutorActorType self, const Envelope &e) {

  auto m = e.getMessage();
  auto &mRef = *m;

  if (typeid(mRef) == typeid(CompleteMessage)) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Operator Complete  |  source: {}",
				 self->id(),
				 self->name(),
				 to_string(self->current_sender()));

	auto completeMessage = std::static_pointer_cast<CompleteMessage>(m);

	self->state.operatorDirectory.setComplete(completeMessage->sender());

	if (self->state.operatorDirectory.allComplete()) {

	  /*
	   * TODO: Need to capture query output here
	   */
	  auto tupleSet = TupleSet2::make();

	  self->state.stopTime = std::chrono::steady_clock::now();

	  resolvePromise(self, tupleSet);

	  self->quit(exit_reason::normal);
	}
  } else {
	auto error = ::caf::make_error(sec::unexpected_message,
								   fmt::format("Envelope contained unexpected message of type '{}'",
											   typeid(mRef).name()));
	self->call_error_handler(error);
  }
}

void boot2(OperatorGraph &g, const QueryExecutorActorType self) {

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

  for (const auto &op:  g.getOperators()) {
	auto entry = OperatorDirectoryEntry(op.second->op()->name(),
										op.second->op()->actorHandle(),
										false);
	self->state.operatorDirectory.insert(entry);
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

void start(OperatorGraph &g, const QueryExecutorActorType self) {

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

	auto sm = std::make_shared<StartMessage>(actorHandles, self->name());

	self->anon_send(op->actorHandle(), normal::core::message::Envelope(sm));
  }
}

void resolvePromise(const QueryExecutorActorType self,
					const std::shared_ptr<TupleSet2> &tupleSet) {
  if (self->state.promise.pending()) {
	auto error = caf::make_error(sec::runtime_error, "Cannot resolve promise  |  Promise is not pending");
	self->call_error_handler(error);
  }

  tl::expected<std::shared_ptr<TupleSet2>, std::string> expected{tupleSet};
  self->state.promise.deliver(expected);
}

void rejectPromise(const QueryExecutorActorType self, const std::string &message) {
  if (self->state.promise.pending()) {
	auto error = caf::make_error(sec::runtime_error, "Cannot reject promise  |  Promise is not pending");
	self->call_error_handler(error);
  }

  tl::expected<std::shared_ptr<TupleSet2>, std::string>
	  expected{tl::make_unexpected(message)};
  self->state.promise.deliver(expected);
}

}

namespace normal::core {

QueryExecutorActor::behavior_type queryExecutorBehaviour(QueryExecutorActorType self, std::string name) {

  self->state.name = std::move(name);

  SPDLOG_DEBUG("[Actor {} ('{}')]  Query Executor Actor Spawn", self->id(), self->name());

  setDefaultHandlers(*self);

  self->set_exit_handler([=](const ::caf::exit_msg &m) {
	onActorExit(self, m);
  });

  return {
	  [=](ExecuteAtom, const std::shared_ptr<OperatorGraph> &operatorGraph) {
		return onExecute(self, operatorGraph);
	  },
	  [=](const Envelope &e) {
		onComplete(self, e);
	  }
  };
}

}
