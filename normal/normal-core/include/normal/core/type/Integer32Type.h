//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER32TYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER32TYPE_H

#include <arrow/api.h>

#include <normal/core/type/Type.h>

namespace normal::core::type {

class Integer32Type: public Type {
private:

public:
  explicit Integer32Type() : Type("Int32") {}
  Integer32Type(const Integer32Type&) = default;
  Integer32Type& operator=(const Integer32Type&) = default;

  std::string asGandivaTypeString() override {
	return "INT";
  }

  std::shared_ptr<arrow::DataType> asArrowType() override {
	return arrow::int32();
  }

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Integer32Type& type) {
    return f.apply(type.name());
  }

};

std::shared_ptr<Type> integer32Type();

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER32TYPE_H
