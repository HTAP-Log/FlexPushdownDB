//
// Created by matt on 24/8/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_QUERYACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_QUERYACTOR_H


#include <caf/all.hpp>
#include <utility>

#include <normal/core/Actors.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

using namespace caf;
using namespace normal::core::graph;

namespace normal::core {

using ExecuteAtom = atom_constant<atom("execute")>;
using CompleteAtom = atom_constant<atom("complete")>;

using QueryActor = ::caf::typed_actor<reacts_to<ExecuteAtom, std::shared_ptr<OperatorGraph>>,
									  reacts_to<Envelope>>;

struct QueryActorState {
  std::string name = "<undefined>";
  ::caf::strong_actor_ptr sender;
  std::weak_ptr<OperatorGraph> g;
  std::chrono::steady_clock::time_point startTime;
  std::chrono::steady_clock::time_point stopTime;
};

QueryActor::behavior_type queryBehaviour(QueryActor::stateful_pointer <QueryActorState> self, std::string name);

void boot2(OperatorGraph &g, QueryActor::stateful_pointer <QueryActorState> queryActor);
void start(OperatorGraph &g, QueryActor::stateful_pointer <QueryActorState> queryActor);

}



CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<OperatorGraph>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<TupleSet2>);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_QUERYACTOR_H
