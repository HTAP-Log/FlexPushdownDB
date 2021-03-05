//
// Created by matt on 20/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEOPERATOR_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEOPERATOR_H

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/TupleMessage.h>
#include <queue>

using namespace normal::core;
using namespace normal::core::message;

namespace normal::pushdown::merge {

class Merge : public Operator {

public:

  explicit Merge(const std::string &Name, long queryId);
  Merge() = default;
  Merge(const Merge&) = default;
  Merge& operator=(const Merge&) = default;

  static std::shared_ptr<Merge> make(const std::string &Name, long queryId = 0);

  void onReceive(const Envelope &msg) override;

  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  void setLeftProducer(const std::shared_ptr<Operator> &leftProducer);
  void setRightProducer(const std::shared_ptr<Operator> &rightProducer);

private:

  void merge();

  std::string leftProducerName_;
  std::string rightProducerName_;

  std::list<std::shared_ptr<TupleSet2>> leftTupleSets_;
  std::list<std::shared_ptr<TupleSet2>> rightTupleSets_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Merge& op) {
    return f.object(op).fields(f.field("leftProducerName", op.leftProducerName_),
                               f.field("rightProducerName", op.rightProducerName_),
                               f.field("name", op.name()),
                               f.field("type", op.getType()),
                               f.field("opContext", op.getOpContext()),
                               f.field("producers", op.getProducers()),
                               f.field("consumers", op.getConsumers()));
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEOPERATOR_H
