//
// Created by matt on 7/7/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H

#include <memory>
#include <string>

#include <caf/all.hpp>
#include <tl/expected.hpp>

#include <normal/core/Operator.h>
#include <normal/core/OperatorContext.h>
#include <normal/core/OperatorDirectory.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/Forward.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/pushdown/collate/Collate2.h>

using namespace normal::core;
using namespace normal::core::cache;
using namespace normal::pushdown;

namespace normal::core::graph {

// TODO: This eventually should be a globally unique name
inline constexpr const char *GraphRootActorName = "root";

/**
 * Executable graph of operators
 */
class OperatorGraph {

public:
  OperatorGraph(long id, const std::shared_ptr<OperatorManager>& operatorManager);
  ~OperatorGraph();
  static std::shared_ptr<OperatorGraph> make(const std::shared_ptr<OperatorManager>& operatorManager);
  void put(const std::shared_ptr<Operator> &def);
  void start();
  void join();
  void boot();
  void close();
  tl::expected<std::shared_ptr<TupleSet2>, std::string> execute();
  void write_graph(const std::string &file);
  std::shared_ptr<Operator> getOperator(const std::string &);
  tl::expected<long, std::string> getElapsedTime();
  std::pair<size_t, size_t> getBytesTransferred();
  size_t getNumRequests();
  std::pair<size_t, size_t> getGetTransferConvertTimesNS();
  std::pair<size_t, size_t> getSelectTransferConvertTimesNS();
  std::string showMetrics();
  [[nodiscard]] const long &getId() const;
  void setNode(const node_id &node);
  void setPlacements(const std::unordered_map<std::shared_ptr<core::Operator>, int, core::OperatorPointerHash, core::OperatorPointerPredicate> &placements);
  const std::shared_ptr<Collate> &getLegacyCollateOperator() const;

private:
  long id_;
  OperatorDirectory operatorDirectory_;
  std::weak_ptr<OperatorManager> operatorManager_;
  std::shared_ptr<caf::scoped_actor> rootActor_;
  CollateActor collateActorHandle_;
  std::shared_ptr<Collate> legacyCollateOperator_;

  std::chrono::steady_clock::time_point startTime_;
  std::chrono::steady_clock::time_point stopTime_;

  /* Distributed nodes */
  std::optional<node_id> node_;
  std::unordered_map<std::shared_ptr<core::Operator>, int, core::OperatorPointerHash, core::OperatorPointerPredicate> placements_;

  caf::actor localSpawn(const std::shared_ptr<Operator>&);
  caf::actor remoteSpawn(const std::shared_ptr<Operator>&);
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H
