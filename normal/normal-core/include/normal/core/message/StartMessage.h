//
// Created by matt on 5/1/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
#define NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H

#include <vector>

#include <normal/core/OperatorConnection.h>

#include <normal/core/message/Message.h>

namespace normal::core::message {

/**
 * Message sent to operators to tell them to start doing their "thing"
 */
class StartMessage : public Message {

public:
  explicit StartMessage(std::string from);
  StartMessage() = default;
  StartMessage(const StartMessage&) = default;
  StartMessage& operator=(const StartMessage&) = default;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StartMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type()),
                                f.field("sender", msg.sender()));
  }
};

}

#endif //NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
