//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H

#include <memory>
#include <unordered_map>

#include <tl/expected.hpp>

#include <normal/core/Operator.h>

namespace normal::plan {

/**
 * A physical query plan
 *
 * At the moment a collection of connected operators.
 */
class PhysicalPlan {

public:
  PhysicalPlan(){};

  void put(std::shared_ptr<core::Operator> operator_, int placement);

  [[nodiscard]] const std::unordered_map<std::shared_ptr<core::Operator>, int, core::OperatorPointerHash, core::OperatorPointerPredicate> &
  getPlacements() const;

private:
  std::unordered_map<std::shared_ptr<core::Operator>, int, core::OperatorPointerHash, core::OperatorPointerPredicate> placements_;
};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
