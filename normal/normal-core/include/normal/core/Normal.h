//
// Created by matt on 16/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_NORMAL_H
#define NORMAL_NORMAL_CORE_SRC_NORMAL_H

#include <memory>

#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/core/SystemActor.h>

using namespace normal::core::graph;

namespace normal::core {

constexpr bool EnableV2 = true;

/**
 * Placeholder for an eventual API
 */
class Normal {

public:
  Normal();

  static std::shared_ptr<Normal> start();
  void stop();

  std::shared_ptr<OperatorGraph> createQuery();
  const std::shared_ptr<OperatorManager> &getOperatorManager() const;

  tl::expected<std::shared_ptr<TupleSet2>, std::string> execute(const std::shared_ptr<OperatorGraph>& g) const;

private:
  std::shared_ptr<OperatorManager> operatorManager_;

  caf::actor_system_config actorSystemConfig_;
  std::unique_ptr<::caf::actor_system> actorSystem_;

  std::unique_ptr<::caf::scoped_actor> clientActor_;
  SystemActor systemActor_;

  tl::expected<QueryExecutorActor, std::string> makeQueryExecutor() const;
  tl::expected<std::shared_ptr<TupleSet2>, std::string> executeQuery(const QueryExecutorActor &queryExecutor,
																	 const std::shared_ptr<OperatorGraph> &operatorGraph) const;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_NORMAL_H
