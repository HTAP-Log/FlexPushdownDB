//
// Created by matt on 26/8/20.
//

#include "ErrorOperator.h"

using namespace normal::core;
using namespace normal::core::message;

ErrorOperator::ErrorOperator(const std::string &Name) : Operator(Name, "Error") {}

void ErrorOperator::onReceive(const Envelope &msg) {

  auto m = msg.getMessage();
  auto &mRef = *m;

  SPDLOG_DEBUG("Received Message  |  message type: '{}'", typeid(mRef).name());

  throw std::runtime_error("Error");
}