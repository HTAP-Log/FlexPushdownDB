//
// Created by matt on 21/8/20.
//

#include "SystemActor.h"

using namespace normal::core;

SystemActor::behavior_type normal::core::systemBehaviour(SystemActor::stateful_pointer <SystemActorState> self) {

  setDefaultHandlers(*self);

  self->set_exit_handler([=](const exit_msg &m) {
	SPDLOG_DEBUG("Actor Exit  |  actor: {} ('{}'), reason: {}, source: {}", self->id(),
				 self->name(), to_string(m.reason), m.source.id());

	for (const auto &queryActor: self->state.queryActors) {
	  self->send_exit(queryActor, m.reason);
	}

	self->send_exit(self->state.segmentCacheActor, m.reason);
	self->quit(m.reason);
  });

  self->set_down_handler([=](const down_msg &m) {
	SPDLOG_DEBUG("Actor Down  |  actor: {} ('{}'), reason: {}, source: {}", self->id(),
				 self->name(), to_string(m.reason), m.source.id());

	// If its a query, remove it from the list

	// If its something else, abort

  });

  self->state.segmentCacheActor = self->spawn(segmentCacheBehaviour);

  return {
	  [=](StartAtom) {
		SPDLOG_DEBUG("Start");
	  },
	  [=](StopAtom) {
		SPDLOG_DEBUG("Stop");
	  },
	  [=](MakeQueryAtom) {
		SPDLOG_ERROR("MakeQuery  |  actor: {} ('{}'), sender: {}", self->id(), self->name(), self->current_sender()->id());

		auto queryId = ++(self->state.lastQueryId);
		auto queryActorName = fmt::format("query-{}", queryId);

		auto queryActor = self->state.queryActors.emplace_back(self->spawn(queryBehaviour, queryActorName));

		self->monitor(queryActor);

		return queryActor;
	  }
  };
}
