//
// Created by Yifei Yang on 2/28/21.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SERIALIZATION_PARTITIONSER_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SERIALIZATION_PARTITIONSER_H

#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include <normal/connector/partition/Partition.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <normal/connector/local-fs/LocalFilePartition.h>

using PartitionPtr = std::shared_ptr<Partition>;

CAF_BEGIN_TYPE_ID_BLOCK(Partition, normal::util::Partition_first_custom_type_id)
CAF_ADD_TYPE_ID(Partition, (PartitionPtr))
CAF_ADD_TYPE_ID(Partition, (S3SelectPartition))
CAF_ADD_TYPE_ID(Partition, (LocalFilePartition))
CAF_END_TYPE_ID_BLOCK(Partition)

namespace caf {

template<>
struct variant_inspector_traits<PartitionPtr> {
  using value_type = PartitionPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<S3SelectPartition>,
          type_id_v<LocalFilePartition>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->type() == "S3SelectPartition")
      return 1;
    else
      return 2;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(static_cast<S3SelectPartition &>(*x));
      case 2:
        return f(static_cast<LocalFilePartition &>(*x));
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
      case type_id_v<S3SelectPartition>: {
        auto tmp = S3SelectPartition{};
        continuation(tmp);
        return true;
      }
      case type_id_v<LocalFilePartition>: {
        auto tmp = LocalFilePartition{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<PartitionPtr> : variant_inspector_access<PartitionPtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SERIALIZATION_PARTITIONSER_H
