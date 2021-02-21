//
// Created by matt on 16/4/20.
//

#include <normal/plan/PhysicalPlan.h>

using namespace normal::plan;

void PhysicalPlan::put(std::shared_ptr<normal::core::Operator> operator_, int placement) {
  placements_.emplace(operator_, placement);
}

const std::unordered_map<std::shared_ptr<normal::core::Operator>, int, normal::core::OperatorPointerHash,
normal::core::OperatorPointerPredicate> &
PhysicalPlan::getPlacements() const {
  return placements_;
}

