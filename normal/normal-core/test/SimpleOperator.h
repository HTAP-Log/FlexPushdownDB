//
// Created by matt on 26/8/20.
//

#ifndef NORMAL_NORMAL_CORE_TEST_SIMPLEOPERATOR_H
#define NORMAL_NORMAL_CORE_TEST_SIMPLEOPERATOR_H

#include <normal/core/Operator.h>

namespace normal::core {

class SimpleOperator : public Operator {
public:
  explicit SimpleOperator(const std::string &Name);
  void onReceive(const message::Envelope &msg) override;
};

}

#endif //NORMAL_NORMAL_CORE_TEST_SIMPLEOPERATOR_H
