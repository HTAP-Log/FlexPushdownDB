//
// Created by matt on 5/12/19.
//

#include "normal/core/Operator.h"

#include <cassert>               // for assert
#include <utility>                // for move

#include "normal/core/Globals.h"
#include <normal/core/message/Envelope.h>
#include "normal/core/message/Message.h"  // for Message

namespace normal::core {

Operator::Operator(std::string name, std::string type, long queryId) :
    name_(std::move(name)),
    type_(std::move(type)),
    queryId_(queryId) {
}

std::string &Operator::getType() {
  return type_;
}

std::string &Operator::name() {
  return name_;
}

void Operator::produce(const std::shared_ptr<Operator> &op) {
  consumers_.emplace(op->name(), op->name());
}

void Operator::consume(const std::shared_ptr<Operator> &op) {
  producers_.emplace(op->name(), op->name());
}

std::map<std::string, std::string> Operator::consumers() {
  return consumers_;
}

std::map<std::string, std::string> Operator::producers() {
  return producers_;
}

std::shared_ptr<OperatorContext> Operator::ctx() {
  return opContext_;
}

std::shared_ptr<OperatorContext> Operator::weakCtx() {
  return opContext_;
}

void Operator::create(const std::shared_ptr<OperatorContext>& ctx) {
  assert (ctx);

  SPDLOG_DEBUG("Creating operator  |  name: '{}'", this->name_);

  opContext_ = ctx;
}

void Operator::setName(const std::string &Name) {
  name_ = Name;
}
void Operator::destroyActor() {
  opContext_->destroyActorHandles();
}

long Operator::getQueryId() const {
  return queryId_;
}

size_t Operator::hash() {
  return std::hash<std::string>()(name_);
}

std::shared_ptr<OperatorContext> &Operator::getOpContext() {
  return opContext_;
}

std::map<std::string, std::string> &Operator::getProducers() {
  return producers_;
}

std::map<std::string, std::string> &Operator::getConsumers() {
  return consumers_;
}

} // namespace

