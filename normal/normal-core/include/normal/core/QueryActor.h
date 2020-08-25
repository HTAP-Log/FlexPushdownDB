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

using namespace caf;
using namespace normal::core::graph;

namespace normal::core {

using ExecuteAtom = atom_constant<atom("execute")>;

using QueryActor = ::caf::typed_actor<replies_to<ExecuteAtom, std::shared_ptr<OperatorGraph>>::with<std::shared_ptr<TupleSet2>>>;

struct QueryActorState {
  std::string name = "<undefined>";
};

QueryActor::behavior_type queryBehaviour(QueryActor::stateful_pointer <QueryActorState> self, std::string name);

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<OperatorGraph>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<TupleSet2>);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_QUERYACTOR_H
