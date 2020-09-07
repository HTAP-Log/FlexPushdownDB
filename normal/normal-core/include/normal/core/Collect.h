//
// Created by matt on 7/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_COLLECT_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_COLLECT_H


#include <string>
#include <memory>                  // for unique_ptr

#include <arrow/api.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "normal/core/Operator.h"
#include "normal/core/OperatorContext.h"
#include "normal/tuple/TupleSet.h"

namespace normal::tuple {
class TupleSet;
}

namespace normal::core {

class Collect : public normal::core::Operator {

private:
  long queryId_;
  std::shared_ptr<TupleSet> tuples_;
  void onStart();

  void onComplete(const normal::core::message::CompleteMessage &message);
  void onTuple(const normal::core::message::TupleMessage& message);
  void onReceive(const normal::core::message::Envelope &message) override;

public:
  Collect(std::string name, long queryId);
  ~Collect() override = default;
  void show();
  std::shared_ptr<TupleSet> tuples();

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_COLLECT_H
