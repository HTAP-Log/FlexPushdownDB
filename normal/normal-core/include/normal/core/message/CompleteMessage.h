//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H
#define NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H

#include <vector>

#include "normal/core/message/Message.h"

namespace normal::core::message {
/**
 * Message fired when an operator completes its work
 */
class CompleteMessage : public Message {

public:
  explicit CompleteMessage(std::string sender);
  CompleteMessage() = default;
  CompleteMessage(const CompleteMessage&) = default;
  CompleteMessage& operator=(const CompleteMessage&) = default;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CompleteMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type()),
                                f.field("sender", msg.sender()));
  }
};

}

#endif //NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H
