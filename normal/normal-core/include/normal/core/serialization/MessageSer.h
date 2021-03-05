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
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/cache/WeightRequestMessage.h>
#include <normal/core/cache/CacheMetricsMessage.h>

using namespace ::normal::core::message;
using namespace ::normal::core::cache;

using MessagePtr = std::shared_ptr<Message>;

CAF_BEGIN_TYPE_ID_BLOCK(Message, normal::util::Message_first_custom_type_id)
CAF_ADD_TYPE_ID(Message, (MessagePtr))
CAF_ADD_TYPE_ID(Message, (StartMessage))
CAF_ADD_TYPE_ID(Message, (ConnectMessage))
CAF_ADD_TYPE_ID(Message, (CompleteMessage))
CAF_ADD_TYPE_ID(Message, (TupleMessage))
// The following are explicit message types, so have to implement `inspect` for concrete derived shared_ptr type,
// implementing for base share_ptr type in this variant-based scheme doesn't work
CAF_ADD_TYPE_ID(Message, (LoadRequestMessage))
CAF_ADD_TYPE_ID(Message, (LoadResponseMessage))
CAF_ADD_TYPE_ID(Message, (StoreRequestMessage))
CAF_ADD_TYPE_ID(Message, (WeightRequestMessage))
CAF_ADD_TYPE_ID(Message, (CacheMetricsMessage))
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
          type_id_v<TupleMessage>,
//          type_id_v<LoadRequestMessage>,
//          type_id_v<LoadResponseMessage>,
//          type_id_v<StoreRequestMessage>,
//          type_id_v<WeightRequestMessage>,
//          type_id_v<CacheMetricsMessage>
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
//    else if (x->type() == "LoadRequestMessage")
//      return 5;
//    else if (x->type() == "LoadResponseMessage")
//      return 6;
//    else if (x->type() == "StoreRequestMessage")
//      return 7;
//    else if (x->type() == "WeightRequestMessage")
//      return 8;
//    else if (x->type() == "CacheMetricsMessage")
//      return 9;
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
      case 4:
        return f(static_cast<TupleMessage &>(*x));
//      case 5:
//        return f(static_cast<LoadRequestMessage &>(*x));
//      case 6:
//        return f(static_cast<LoadResponseMessage &>(*x));
//      case 7:
//        return f(static_cast<StoreRequestMessage &>(*x));
//      case 8:
//        return f(static_cast<WeightRequestMessage &>(*x));
//      case 9:
//        return f(static_cast<CacheMetricsMessage &>(*x));
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
//      case type_id_v<LoadRequestMessage>: {
//        auto tmp = LoadRequestMessage{};
//        continuation(tmp);
//        return true;
//      }
//      case type_id_v<LoadResponseMessage>: {
//        auto tmp = LoadResponseMessage{};
//        continuation(tmp);
//        return true;
//      }
//      case type_id_v<StoreRequestMessage>: {
//        auto tmp = StoreRequestMessage{};
//        continuation(tmp);
//        return true;
//      }
//      case type_id_v<WeightRequestMessage>: {
//        auto tmp = WeightRequestMessage{};
//        continuation(tmp);
//        return true;
//      }
//      case type_id_v<CacheMetricsMessage>: {
//        auto tmp = CacheMetricsMessage{};
//        continuation(tmp);
//        return true;
//      }
    }
  }
};

template <>
struct inspector_access<MessagePtr> : variant_inspector_access<MessagePtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_MESSAGESER_H
