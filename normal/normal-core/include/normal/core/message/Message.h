//
// Created by matt on 9/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H

#include <string>
#include <caf/all.hpp>

namespace normal::core::message {

/**
 * Base class for messages
 */
class Message {

private:
  std::string type_;
  std::string sender_;

public:
  explicit Message(std::string type, std::string sender);
  Message() = default;
  Message(const Message&) = default;
  Message& operator=(const Message&) = default;
  virtual ~Message() = default;
  [[nodiscard]] std::string type() const;
  [[nodiscard]] std::string sender() const;

  // caf inspector needs reference get function
  std::string& type();
  std::string& sender();
};

} // namespace

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H
