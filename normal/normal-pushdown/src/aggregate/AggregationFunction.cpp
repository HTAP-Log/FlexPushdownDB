//
// Created by matt on 7/3/20.
//

#include <utility>

#include "normal/pushdown/aggregate/AggregationFunction.h"

namespace normal::pushdown::aggregate {

AggregationFunction::AggregationFunction(std::string columnName, std::string type) :
  alias_(std::move(columnName)),
  type_(type) {}

std::string &AggregationFunction::alias() {
  return alias_;
}

std::string &AggregationFunction::type() {
  return type_;
}

}