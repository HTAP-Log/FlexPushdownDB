//
// Created by matt on 21/8/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SYSTEMACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SYSTEMACTOR_H

#include <caf/all.hpp>

#include <normal/core/Actors.h>
#include <normal/core/cache/SegmentCacheActor2.h>
#include <normal/core/graph/OperatorGraph.h>
#include "QueryExecutorActor.h"

using namespace caf;
using namespace normal::core::cache;
using namespace normal::core::graph;

namespace normal::core {

using MakeQueryAtom = atom_constant<atom("make-query")>;

using SystemActor = ::caf::typed_actor<replies_to<MakeQueryAtom>::with<QueryExecutorActor>>;

struct QueryActorMetaData {
  size_t queryId;
  std::string name;
  QueryExecutorActor actor;
};

struct SystemActorState {

  std::string name = "system";

  SegmentCacheActor2 segmentCacheActor;

  size_t lastQueryId = 0;
  std::unordered_map<::caf::actor_addr, QueryActorMetaData> queryExecutorActorsMap;
};

using SystemActorType = SystemActor::stateful_pointer<SystemActorState>;

SystemActor::behavior_type systemBehaviour(SystemActorType self);

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SYSTEMACTOR_H
