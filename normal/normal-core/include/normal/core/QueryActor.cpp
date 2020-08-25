//
// Created by matt on 24/8/20.
//

#include "QueryActor.h"

using namespace normal::core;

QueryActor::behavior_type normal::core::queryBehaviour(QueryActor::stateful_pointer <QueryActorState> self, std::string name) {

  self->state.name = std::move(name);

  setDefaultHandlers(*self);

  return {
	  [=](ExecuteAtom, const std::shared_ptr<OperatorGraph>& g) {
		SPDLOG_DEBUG("Execute  |  actor: {} ('{}'), sender: {}", self->id(), self->name(), self->current_sender()->id());

		self->quit(exit_reason::normal);

		return TupleSet2::make();
	  }
  };
}
