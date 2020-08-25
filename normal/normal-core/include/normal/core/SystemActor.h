//
// Created by matt on 21/8/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SYSTEMACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SYSTEMACTOR_H

#include <caf/all.hpp>

#include <normal/core/Actors.h>
#include <normal/core/cache/SegmentCacheActor2.h>
#include <normal/core/graph/OperatorGraph.h>
#include "QueryActor.h"

using namespace caf;
using namespace normal::core::cache;
using namespace normal::core::graph;

namespace normal::core {

using StartAtom = atom_constant<atom("start")>;
using StopAtom = atom_constant<atom("stop")>;
using MakeQueryAtom = atom_constant<atom("make-query")>;

using SystemActor = ::caf::typed_actor<reacts_to<StartAtom>,
									   reacts_to<StopAtom>,
									   replies_to<MakeQueryAtom>::with<QueryActor>>;

struct SystemActorState {

  std::string name = "system";

  SegmentCacheActor2Actor segmentCacheActor;

  size_t lastQueryId = 0;
  std::vector<QueryActor> queryActors;
};

SystemActor::behavior_type systemBehaviour(SystemActor::stateful_pointer <SystemActorState> self);

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SYSTEMACTOR_H
