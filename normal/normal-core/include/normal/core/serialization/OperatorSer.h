//
// Created by Yifei Yang on 2/28/21.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_OPERATORSER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_OPERATORSER_H

#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include <normal/core/Operator.h>
#include <normal/pushdown/s3/S3Select.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/pushdown/group/Group.h>
#include <normal/pushdown/shuffle/Shuffle.h>

using namespace normal::pushdown;

using OperatorPtr = std::shared_ptr<normal::core::Operator>;

CAF_BEGIN_TYPE_ID_BLOCK(Operator, normal::util::Operator_first_custom_type_id)
CAF_ADD_TYPE_ID(Operator, (OperatorPtr))
CAF_ADD_TYPE_ID(Operator, (S3Select))
CAF_ADD_TYPE_ID(Operator, (join::HashJoinBuild))
CAF_ADD_TYPE_ID(Operator, (join::HashJoinProbe))
CAF_ADD_TYPE_ID(Operator, (normal::pushdown::group::Group))
CAF_ADD_TYPE_ID(Operator, (shuffle::Shuffle))
CAF_END_TYPE_ID_BLOCK(Operator)

// Variant-based approach on OperatorPtr
namespace caf {

template<>
struct variant_inspector_traits<OperatorPtr> {
  using value_type = OperatorPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<S3Select>,
          type_id_v<join::HashJoinBuild>,
          type_id_v<join::HashJoinProbe>,
          type_id_v<normal::pushdown::group::Group>,
          type_id_v<shuffle::Shuffle>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == "S3Select")
      return 1;
    else if (x->getType() == "HashJoinBuild")
      return 2;
    else if (x->getType() == "HashJoinProbe")
      return 3;
    else if (x->getType() == "Group")
      return 4;
    else if (x->getType() == "Shuffle")
      return 5;
    else return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(static_cast<S3Select &>(*x));
      case 2:
        return f(static_cast<join::HashJoinBuild &>(*x));
      case 3:
        return f(static_cast<join::HashJoinProbe &>(*x));
      case 4:
        return f(static_cast<normal::pushdown::group::Group &>(*x));
      case 5:
        return f(static_cast<shuffle::Shuffle &>(*x));
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
      case type_id_v<S3Select>: {
        auto tmp = S3Select{};
        continuation(tmp);
        return true;
      }
      case type_id_v<join::HashJoinBuild>: {
        auto tmp = join::HashJoinBuild{};
        continuation(tmp);
        return true;
      }
      case type_id_v<join::HashJoinProbe>: {
        auto tmp = join::HashJoinProbe{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::pushdown::group::Group>: {
        auto tmp = normal::pushdown::group::Group{};
        continuation(tmp);
        return true;
      }
      case type_id_v<shuffle::Shuffle>: {
        auto tmp = shuffle::Shuffle{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<OperatorPtr> : variant_inspector_access<OperatorPtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_SERIALIZATION_OPERATORSER_H
