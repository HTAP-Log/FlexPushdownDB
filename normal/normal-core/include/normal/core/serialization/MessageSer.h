//
// Created by Yifei Yang on 2/28/21.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_MESSAGESER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_MESSAGESER_H

#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include <normal/core/message/Message.h>
#include <normal/core/message/StartMessage.h>
#include <normal/core/message/ConnectMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/TupleMessage.h>

using namespace normal::core::message;
using MessagePtr = std::shared_ptr<Message>;

CAF_BEGIN_TYPE_ID_BLOCK(Message, normal::util::Message_first_custom_type_id)
CAF_ADD_TYPE_ID(Message, (MessagePtr))
CAF_ADD_TYPE_ID(Message, (StartMessage))
CAF_ADD_TYPE_ID(Message, (ConnectMessage))
CAF_ADD_TYPE_ID(Message, (CompleteMessage))
CAF_ADD_TYPE_ID(Message, (TupleMessage))
CAF_END_TYPE_ID_BLOCK(Message)

// Variant-based approach on OperatorPtr
namespace caf {

template<>
struct variant_inspector_traits<MessagePtr> {
  using value_type = MessagePtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<StartMessage>,
          type_id_v<ConnectMessage>,
          type_id_v<CompleteMessage>,
          type_id_v<TupleMessage>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->type() == "StartMessage")
      return 1;
    else if (x->type() == "ConnectMessage")
      return 2;
    else if (x->type() == "CompleteMessage")
      return 3;
    else if (x->type() == "TupleMessage")
      return 4;
    else return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(static_cast<StartMessage &>(*x));
      case 2:
        return f(static_cast<ConnectMessage &>(*x));
      case 3:
        return f(static_cast<CompleteMessage &>(*x));
      case 4:{
        auto a = f(static_cast<TupleMessage &>(*x));
        return a;}
      default: {
        none_t dummy;
        return f(dummy);
      }
    }
  }

  // Assigns a value to x.
  template<class U>
  static void assign(value_type &x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template<class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<StartMessage>: {
        auto tmp = StartMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<ConnectMessage>: {
        auto tmp = ConnectMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<CompleteMessage>: {
        auto tmp = CompleteMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<TupleMessage>: {
        auto tmp = TupleMessage{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<MessagePtr> : variant_inspector_access<MessagePtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_MESSAGESER_H
