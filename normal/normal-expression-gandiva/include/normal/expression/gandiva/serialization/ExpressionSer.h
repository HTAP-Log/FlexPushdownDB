//
// Created by Yifei Yang on 2/28/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SERIALIZATION_EXPRESSIONSER_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SERIALIZATION_EXPRESSIONSER_H

#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/gandiva/Add.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/Divide.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/StringLiteral.h>
#include <normal/expression/gandiva/Subtract.h>

using namespace normal::expression::gandiva;
using ExpressionPtr = std::shared_ptr<normal::expression::gandiva::Expression>;


CAF_BEGIN_TYPE_ID_BLOCK(Expression, normal::util::Expression_first_custom_type_id)
CAF_ADD_TYPE_ID(Expression, (ExpressionPtr))
CAF_ADD_TYPE_ID(Expression, (Add))
CAF_ADD_TYPE_ID(Expression, (And))
CAF_ADD_TYPE_ID(Expression, (Cast))
CAF_ADD_TYPE_ID(Expression, (normal::expression::gandiva::Column))
CAF_ADD_TYPE_ID(Expression, (Divide))
CAF_ADD_TYPE_ID(Expression, (EqualTo))
CAF_ADD_TYPE_ID(Expression, (GreaterThan))
CAF_ADD_TYPE_ID(Expression, (GreaterThanOrEqualTo))
CAF_ADD_TYPE_ID(Expression, (LessThan))
CAF_ADD_TYPE_ID(Expression, (LessThanOrEqualTo))
CAF_ADD_TYPE_ID(Expression, (Multiply))
//CAF_ADD_TYPE_ID(Expression, (NumericLiteral<ARROW_TYPE>))
//CAF_ADD_TYPE_ID(Expression, (NumericLiteral<arrow::Int32Type>))
CAF_ADD_TYPE_ID(Expression, (Or))
CAF_ADD_TYPE_ID(Expression, (StringLiteral))
CAF_ADD_TYPE_ID(Expression, (Subtract))
CAF_END_TYPE_ID_BLOCK(Expression)

// Variant-based approach on OperatorPtr
namespace caf {

template<>
struct variant_inspector_traits<ExpressionPtr> {
  using value_type = ExpressionPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<Add>,
          type_id_v<And>,
          type_id_v<Cast>,
          type_id_v<normal::expression::gandiva::Column>,
          type_id_v<Divide>,
          type_id_v<EqualTo>,
          type_id_v<GreaterThan>,
          type_id_v<GreaterThanOrEqualTo>,
          type_id_v<LessThan>,
          type_id_v<LessThanOrEqualTo>,
          type_id_v<Multiply>,
          type_id_v<Or>,
          type_id_v<StringLiteral>,
          type_id_v<Subtract>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->expType() == "Add")
      return 1;
    else if (x->expType() == "And")
      return 2;
    else if (x->expType() == "Cast")
      return 3;
    else if (x->expType() == "Column")
      return 4;
    else if (x->expType() == "Divide")
      return 5;
    else if (x->expType() == "EqualTo")
      return 6;
    else if (x->expType() == "GreaterThan")
      return 7;
    else if (x->expType() == "GreaterThanOrEqualTo")
      return 8;
    else if (x->expType() == "LessThan")
      return 9;
    else if (x->expType() == "LessThanOrEqualTo")
      return 10;
    else if (x->expType() == "Multiply")
      return 11;
    else if (x->expType() == "Or")
      return 12;
    else if (x->expType() == "StringLiteral")
      return 13;
    else if (x->expType() == "Subtract")
      return 14;
    else return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(static_cast<Add &>(*x));
      case 2:
        return f(static_cast<And &>(*x));
      case 3:
        return f(static_cast<Cast &>(*x));
      case 4:
        return f(static_cast<normal::expression::gandiva::Column &>(*x));
      case 5:
        return f(static_cast<Divide &>(*x));
      case 6:
        return f(static_cast<EqualTo &>(*x));
      case 7:
        return f(static_cast<GreaterThan &>(*x));
      case 8:
        return f(static_cast<GreaterThanOrEqualTo &>(*x));
      case 9:
        return f(static_cast<LessThan &>(*x));
      case 10:
        return f(static_cast<LessThanOrEqualTo &>(*x));
      case 11:
        return f(static_cast<Multiply &>(*x));
      case 12:
        return f(static_cast<Or &>(*x));
      case 13:
        return f(static_cast<StringLiteral &>(*x));
      case 14:
        return f(static_cast<Subtract &>(*x));
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
      case type_id_v<Add>: {
        auto tmp = Add{};
        continuation(tmp);
        return true;
      }
      case type_id_v<And>: {
        auto tmp = And{};
        continuation(tmp);
        return true;
      }
      case type_id_v<Cast>: {
        auto tmp = Cast{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::expression::gandiva::Column>: {
        auto tmp = normal::expression::gandiva::Column{};
        continuation(tmp);
        return true;
      }
      case type_id_v<EqualTo>: {
        auto tmp = EqualTo{};
        continuation(tmp);
        return true;
      }
      case type_id_v<GreaterThan>: {
        auto tmp = GreaterThan{};
        continuation(tmp);
        return true;
      }
      case type_id_v<GreaterThanOrEqualTo>: {
        auto tmp = GreaterThanOrEqualTo{};
        continuation(tmp);
        return true;
      }
      case type_id_v<LessThan>: {
        auto tmp = LessThan{};
        continuation(tmp);
        return true;
      }
      case type_id_v<LessThanOrEqualTo>: {
        auto tmp = LessThanOrEqualTo{};
        continuation(tmp);
        return true;
      }
      case type_id_v<Multiply>: {
        auto tmp = Multiply{};
        continuation(tmp);
        return true;
      }
      case type_id_v<Or>: {
        auto tmp = Or{};
        continuation(tmp);
        return true;
      }
      case type_id_v<StringLiteral>: {
        auto tmp = StringLiteral{};
        continuation(tmp);
        return true;
      }
      case type_id_v<Subtract>: {
        auto tmp = Subtract{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<ExpressionPtr> : variant_inspector_access<ExpressionPtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_SERIALIZATION_EXPRESSIONSER_H
