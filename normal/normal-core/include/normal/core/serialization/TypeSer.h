//
// Created by Yifei Yang on 3/2/21.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_TYPESER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_TYPESER_H

#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include <normal/core/type/Type.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/core/type/Integer64Type.h>
#include <normal/core/type/Float64Type.h>
#include <normal/core/type/DecimalType.h>

using namespace normal::core::type;
using TypePtr = std::shared_ptr<Type>;

CAF_BEGIN_TYPE_ID_BLOCK(Type, normal::util::Type_first_custom_type_id)
CAF_ADD_TYPE_ID(Type, (TypePtr))
CAF_ADD_TYPE_ID(Type, (Integer32Type))
CAF_ADD_TYPE_ID(Type, (Integer64Type))
CAF_ADD_TYPE_ID(Type, (Float64Type))
CAF_ADD_TYPE_ID(Type, (DecimalType))
CAF_END_TYPE_ID_BLOCK(Type)

// Variant-based approach on OperatorPtr
namespace caf {

template<>
struct variant_inspector_traits<TypePtr> {
  using value_type = TypePtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<Integer32Type>,
          type_id_v<Integer64Type>,
          type_id_v<Float64Type>,
          type_id_v<DecimalType>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->name() == "Int32")
      return 1;
    else if (x->name() == "Int64")
      return 2;
    else if (x->name() == "Float64")
      return 3;
    else if (x->name() == "Decimal")
      return 4;
    else return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(static_cast<Integer32Type &>(*x));
      case 2:
        return f(static_cast<Integer64Type &>(*x));
      case 3:
        return f(static_cast<Float64Type &>(*x));
      case 4:
        return f(static_cast<DecimalType &>(*x));
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
      case type_id_v<Integer32Type>: {
        auto tmp = Integer32Type{};
        continuation(tmp);
        return true;
      }
      case type_id_v<Integer64Type>: {
        auto tmp = Integer64Type{};
        continuation(tmp);
        return true;
      }
      case type_id_v<Float64Type>: {
        auto tmp = Float64Type{};
        continuation(tmp);
        return true;
      }
      case type_id_v<DecimalType>: {
        auto tmp = DecimalType{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<TypePtr> : variant_inspector_access<TypePtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_TYPESER_H
