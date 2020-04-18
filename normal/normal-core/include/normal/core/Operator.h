//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATOR_H
#define NORMAL_NORMAL_CORE_SRC_OPERATOR_H

#include <string>
#include <memory>
#include <map>

#include <caf/all.hpp>

#include "normal/core/message/Message.h"
#include "normal/core/OperatorContext.h"
#include "normal/core/message/Envelope.h"

namespace normal::core {

class OperatorContext;

/**
 * Base class for operators
 */
class Operator {

private:
  std::string name_;
  std::string type_;
  std::shared_ptr<OperatorContext> opContext_;
  std::map<std::string, std::shared_ptr<Operator>> producers_;
  std::map<std::string, std::shared_ptr<Operator>> consumers_;
  caf::actor actorHandle_;

public:
  explicit Operator(std::string name, std::string type);
  virtual ~Operator() = 0;

  std::string &name();
  [[nodiscard]] caf::actor actorHandle() const;
  void actorHandle(caf::actor actorId);
  std::shared_ptr<OperatorContext> ctx();
  void setName(const std::string &Name);
  virtual void onReceive(const normal::core::message::Envelope &msg) = 0;

  std::map<std::string, std::shared_ptr<Operator>> producers();
  std::map<std::string, std::shared_ptr<Operator>> consumers();

  void create(std::shared_ptr<OperatorContext> ctx);
  void produce(const std::shared_ptr<Operator> &operator_);
  void consume(const std::shared_ptr<Operator> &operator_);
  const std::string &getType() const;

};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
