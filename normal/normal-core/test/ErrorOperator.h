//
// Created by matt on 26/8/20.
//

#ifndef NORMAL_NORMAL_CORE_TEST_ERROROPERATOR_H
#define NORMAL_NORMAL_CORE_TEST_ERROROPERATOR_H

#include <normal/core/Operator.h>

namespace normal::core {

class ErrorOperator : public Operator {
public:
  explicit ErrorOperator(const std::string &Name);
  void onReceive(const message::Envelope &msg) override;
};

}

#endif //NORMAL_NORMAL_CORE_TEST_ERROROPERATOR_H
