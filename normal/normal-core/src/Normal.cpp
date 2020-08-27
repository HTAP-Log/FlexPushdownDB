//
// Created by matt on 16/12/19.
//

#include "normal/core/Normal.h"

#include <normal/core/Actors.h>
#include <normal/core/SystemActor.h>
#include "normal/core/Globals.h"

namespace normal::core {

Normal::Normal() {

}

std::shared_ptr<Normal> Normal::start() {

  auto n = std::make_shared<Normal>();

  if (EnableV2) {
	n->actorSystem_ = std::make_unique<::caf::actor_system>(n->actorSystemConfig_);

	n->clientActor_ = std::make_unique<caf::scoped_actor>(*n->actorSystem_);

	n->systemActor_ = n->actorSystem_->spawn(systemBehaviour);

	n->operatorManager_ = std::make_shared<OperatorManager>();

  } else {
	n->operatorManager_ = std::make_shared<OperatorManager>();
	n->operatorManager_->boot();
	n->operatorManager_->start();
  }

  return n;
}

void Normal::stop() {
  if (EnableV2) {
	(*clientActor_)->send_exit(systemActor_, exit_reason::user_shutdown);

	clientActor_.reset();

	actorSystem_->await_all_actors_done();
  } else {
	operatorManager_->stop();
  }
}

std::shared_ptr<OperatorGraph> Normal::createQuery() {
  return OperatorGraph::make(operatorManager_);
}

const std::shared_ptr<OperatorManager> &Normal::getOperatorManager() const {
  return operatorManager_;
}

tl::expected<std::shared_ptr<TupleSet2>, std::string> Normal::execute(const std::shared_ptr<OperatorGraph> &g) const {
  return makeQueryExecutor().and_then([&](const auto &e) { return executeQuery(e, g); });
}

tl::expected<QueryExecutorActor, std::string> Normal::makeQueryExecutor() const {
  return request<QueryExecutorActor>(*clientActor_, systemActor_, MakeQueryAtom::value);
}

tl::expected<std::shared_ptr<TupleSet2>, std::string>
Normal::executeQuery(const QueryExecutorActor &queryExecutor,
					 const std::shared_ptr<OperatorGraph> &operatorGraph) const {
  return request<std::shared_ptr<TupleSet2>>(*clientActor_,
											 queryExecutor,
											 ExecuteAtom::value,
											 operatorGraph);
}

}
