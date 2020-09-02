//
// Created by matt on 24/8/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_QUERYEXECUTORACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_QUERYEXECUTORACTOR_H

#include <utility>
#include <string>

#include <caf/all.hpp>

#include <normal/core/graph/OperatorGraph.h>

using namespace caf;
using namespace normal::core::graph;

namespace normal::core {

using ExecuteAtom = atom_constant<atom("execute")>;
using CompleteAtom = atom_constant<atom("complete")>;

using QueryExecutorActor = ::caf::typed_actor<replies_to<ExecuteAtom, std::shared_ptr<OperatorGraph>>
											  ::with<tl::expected<std::shared_ptr<TupleSet2>, std::string>>,
											  reacts_to<Envelope>>;

struct QueryExecutorActorState {
  std::string name;
  ::caf::strong_actor_ptr sender;
  std::weak_ptr<OperatorGraph> operatorGraph;
  OperatorDirectory operatorDirectory;
  ::caf::response_promise promise;

  // TODO: Put these into a queryable data structure.

  std::chrono::steady_clock::time_point startTime;
  std::chrono::steady_clock::time_point stopTime;
};

using QueryExecutorActorType = QueryExecutorActor::stateful_pointer<QueryExecutorActorState>;

QueryExecutorActor::behavior_type queryExecutorBehaviour(QueryExecutorActorType self, std::string operatorGraph);

}

using ExpectedTupleSet = tl::expected<std::shared_ptr<TupleSet2>, std::string>;
using UnexpectedTupleSet = tl::unexpected<std::string>;

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<OperatorGraph>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<TupleSet2>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(ExpectedTupleSet);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_QUERYEXECUTORACTOR_H
