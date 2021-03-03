//
// Created by matt on 8/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER64TYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER64TYPE_H

#include <arrow/api.h>

#include <normal/core/type/Type.h>

namespace normal::core::type {

class Integer64Type: public Type {
private:

public:
  explicit Integer64Type() : Type("Int64") {}
  Integer64Type(const Integer64Type&) = default;
  Integer64Type& operator=(const Integer64Type&) = default;

  std::string asGandivaTypeString() override {
	return "BIGINT";
  }

  std::shared_ptr<arrow::DataType> asArrowType() override {
	return arrow::int64();
  }

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Integer64Type& type) {
    return f.apply(type.name());
  }

};

std::shared_ptr<Type> integer64Type();

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER64TYPE_H
