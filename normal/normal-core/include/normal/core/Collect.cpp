//
// Created by matt on 7/9/20.
//

#include "Collect.h"

namespace normal::core {

void Collect::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  // FIXME: Not the best way to reset the tuples structure
  this->tuples_.reset();
}

Collect::Collect(std::string name, long queryId) :
	Operator(std::move(name), "Collate"),
	queryId_(queryId) {
}

void Collect::onReceive(const normal::core::message::Envelope &message) {
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

void Collect::onComplete(const normal::core::message::CompleteMessage &) {
  if(ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)){
	ctx()->notifyComplete();
  }
}

void Collect::show() {

  assert(tuples_);

  SPDLOG_DEBUG("Collated  |  tupleSet:\n{}", this->name(), tuples_->toString());
}

std::shared_ptr<TupleSet> Collect::tuples() {

  assert(tuples_);

  return tuples_;
}
void Collect::onTuple(const normal::core::message::TupleMessage &message) {

//  SPDLOG_DEBUG("Received tuples");

  if (!tuples_) {
	assert(message.tuples());
	tuples_ = message.tuples();
  } else {
	auto tables = std::vector<std::shared_ptr<arrow::Table>>();
	std::shared_ptr<arrow::Table> table;
	tables.push_back(message.tuples()->table());
	tables.push_back(tuples_->table());
	const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables);
	if (!res.ok())
	  abort();
	tuples_->table(*res);
  }
}

}