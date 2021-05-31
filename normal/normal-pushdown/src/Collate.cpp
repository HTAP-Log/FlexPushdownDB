//
// Created by matt on 5/12/19.
//

#include "normal/pushdown/Collate.h"

#include <vector>                      // for vector

#include <arrow/table.h>               // for ConcatenateTables, Table (ptr ...
#include <arrow/pretty_print.h>

#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "normal/pushdown/Globals.h"

using namespace normal::core;

namespace normal::pushdown {

void Collate::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  // FIXME: Not the best way to reset the tuples structure
  this->tuples_.reset();
}

Collate::Collate(std::string name, long queryId) :
  Operator(std::move(name), "Collate", queryId) {
}

void Collate::onReceive(const normal::core::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void Collate::onComplete(const normal::core::message::CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)){
    if (tables_.size() > 0) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok())
        abort();
      tuples_->table(*res);
      tables_.clear();
    }
	ctx()->notifyComplete();
  }
}

void Collate::show() {

  assert(tuples_);

  SPDLOG_DEBUG("Collated  |  tupleSet:\n{}", this->name(), tuples_->toString());
}

std::shared_ptr<TupleSet> Collate::tuples() {

//  assert(tuples_);
  if (tuples_) {
      SPDLOG_CRITICAL(tuples_->numRows());
  } else {
      SPDLOG_CRITICAL("NO ROWS OUTPUT!");
  }

  return tuples_;
}
void Collate::onTuple(const normal::core::message::TupleMessage &message) {

//  SPDLOG_DEBUG("Received tuples");

  if (!tuples_) {
    assert(message.tuples());
    tuples_ = message.tuples();
  } else {
//    auto tables = std::vector<std::shared_ptr<arrow::Table>>();
//    std::shared_ptr<arrow::Table> table;
    tables_.push_back(message.tuples()->table());
    if (tables_.size() > tablesCutoff_) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok())
        abort();
      tuples_->table(*res);
      tables_.clear();
    }
  }
}

void Collate::setTuples(const std::shared_ptr<TupleSet> &Tuples) {
  tuples_ = Tuples;
}

}