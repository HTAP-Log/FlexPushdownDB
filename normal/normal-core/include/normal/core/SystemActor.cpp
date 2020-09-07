//
// Created by matt on 21/8/20.
//

#include "SystemActor.h"

namespace {

void exitQueryExecutors(SystemActorType self, const error &reason);
void exitSegmentCache(SystemActorType self, const error &reason);
QueryExecutorActor onMakeQuery(SystemActorType self);
void onActorDown(SystemActorType self, const down_msg &m);

void exitQueryExecutors(SystemActorType self, const error &reason) {
  for (const auto &queryActor: self->state.queryExecutorActorsMap) {
	self->send_exit(queryActor.second.actor, reason);
  }
}

void exitSegmentCache(SystemActorType self, const error &reason) {
  self->send_exit(self->state.segmentCacheActor, reason);
}

QueryExecutorActor onMakeQuery(const SystemActorType self) {
  SPDLOG_DEBUG("[Actor {} ('{}')]  Make Query  |  source: {}",
			   self->id(), self->name(), to_string(self->current_sender()));

  auto id = (self->state.lastQueryId)++;
  auto name = fmt::format("query-executor-{}", id);
  auto actor = self->spawn(queryExecutorBehaviour, id, name, self->state.segmentCacheActor);

  self->state.queryExecutorActorsMap.emplace(actor.address(), QueryActorMetaData{id, name, actor});
  self->monitor(actor);

  return actor;
}

void onActorDown(const SystemActorType self, const down_msg &m) {

  auto it = self->state.queryExecutorActorsMap.find(m.source);
  if (it != self->state.queryExecutorActorsMap.end()) {
	if (m.reason != sec::none) {
	  SPDLOG_WARN("[Actor {} ('{}')]  Abnormal Query Executor Down  |  source: {} ('{}'), reason: {}", self->id(),
				  self->name(), to_string(m.source), it->second.name, to_string(m.reason));
	} else {
	  SPDLOG_DEBUG("[Actor {} ('{}')]  Query Executor Down  |  source: {} ('{}'), reason: {}", self->id(),
				   self->name(), to_string(m.source), it->second.name, to_string(m.reason));
	}
	self->state.queryExecutorActorsMap.erase(m.source);
  } else {
	auto msg = fmt::format("[Actor {} ('{}')]  Unexpected Actor Down  |  source: {}, reason: {}", self->id(),
						   self->name(), to_string(m.source), to_string(m.reason));
	SPDLOG_ERROR(msg);
	exitQueryExecutors(self, m.reason);
	exitSegmentCache(self, m.reason);
	self->quit(::caf::make_error(sec::runtime_error, msg));
  }
}

}

namespace normal::core {

SystemActor::behavior_type systemBehaviour(SystemActorType self) {

  SPDLOG_DEBUG("[Actor {} ('{}')]  System Actor Spawn", self->id(), self->name());

  setDefaultHandlers(*self);

  self->set_down_handler([=](const down_msg &m) {
	onActorDown(self, m);
  });

  self->state.segmentCacheActor = self->spawn(segmentCacheBehaviour);
  self->link_to(self->state.segmentCacheActor);

  return {
	  [=](MakeQueryAtom) {
		return onMakeQuery(self);
	  }
  };
}

}
