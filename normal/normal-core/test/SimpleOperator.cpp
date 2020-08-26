//
// Created by matt on 26/8/20.
//

#include "SimpleOperator.h"

using namespace normal::core;
using namespace normal::core::message;

SimpleOperator::SimpleOperator(const std::string &Name) : Operator(Name, "Simple") {}

void SimpleOperator::onReceive(const Envelope &msg) {

  auto m = msg.getMessage();
  auto &mRef = *m;

  SPDLOG_DEBUG("Received Message  |  message type: '{}'", typeid(mRef).name());

  ctx()->notifyComplete();
}


