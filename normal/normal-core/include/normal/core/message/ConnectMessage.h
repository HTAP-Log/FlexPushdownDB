//
// Created by matt on 30/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_MESSAGE_CONNECTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_MESSAGE_CONNECTMESSAGE_H

#include <vector>

#include <normal/core/OperatorConnection.h>

#include "normal/core/message/Message.h"

namespace normal::core::message {

/**
 * Message sent to operators to tell them who they are connected to
 */
class ConnectMessage : public Message {

private:
  std::vector<OperatorConnection> operatorConnections_;

public:
  explicit ConnectMessage(std::vector<OperatorConnection> operatorConnections, std::string from);
  ConnectMessage() = default;
  ConnectMessage(const ConnectMessage&) = default;
  ConnectMessage& operator=(const ConnectMessage&) = default;
  [[nodiscard]] const std::vector<OperatorConnection> &connections() const;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ConnectMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type()),
                                f.field("sender", msg.sender()),
                                f.field("operatorConnections", msg.operatorConnections_));
  }
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_MESSAGE_CONNECTMESSAGE_H
