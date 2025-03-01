//
// Created by matt on 8/5/20.
//

#include "normal/tuple/ColumnBuilder.h"

using namespace normal::tuple;

ColumnBuilder::ColumnBuilder(std::string name, const std::shared_ptr<::arrow::DataType> &type) :name_(std::move(name)) {
  auto arrowStatus = ::arrow::MakeBuilder(::arrow::default_memory_pool(), type, &arrowBuilder_);
}

std::shared_ptr<ColumnBuilder> ColumnBuilder::make(const std::string &name,
												   const std::shared_ptr<::arrow::DataType> &type) {
  return std::make_unique<ColumnBuilder>(name, type);
}

void ColumnBuilder::append(const std::shared_ptr<Scalar> &scalar) {
    // adding int 32 array builder, not sure why this is not implemented.
    if (scalar->type()->id() == ::arrow::Int32Type::type_id) {
        auto rawBuilderPtr = arrowBuilder_.get();
        auto typedArrowBuilder = dynamic_cast<::arrow::Int32Builder*>(rawBuilderPtr);
        auto status = typedArrowBuilder->Append(scalar->value<int>());
    } else if (scalar->type()->id() == ::arrow::Int64Type::type_id) {
        auto rawBuilderPtr = arrowBuilder_.get();
        auto typedArrowBuilder = dynamic_cast<::arrow::Int64Builder*>(rawBuilderPtr);
        auto status = typedArrowBuilder->Append(scalar->value<long>());
    } else if (scalar->type()->id() == ::arrow::StringType::type_id) {
        auto rawBuilderPtr = arrowBuilder_.get();
        auto typedArrowBuilder = dynamic_cast<::arrow::StringBuilder*>(rawBuilderPtr);
        auto status = typedArrowBuilder->Append(scalar->value<std::string>());
    } else if (scalar->type()->id() == ::arrow::BooleanType::type_id) {
        auto rawBuilderPtr = arrowBuilder_.get();
        auto typedArrowBuilder = dynamic_cast<::arrow::BooleanBuilder*>(rawBuilderPtr);
        auto status = typedArrowBuilder->Append(scalar->value<bool>());
    } else {
        throw std::runtime_error(
            "Builder for type '" + scalar->type()->ToString() + "' not implemented yet");
    }
}

std::shared_ptr<Column> ColumnBuilder::finalize() {
  auto status = arrowBuilder_->Finish(&array_);
  auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(array_);
  return std::make_shared<Column>(name_, chunkedArray);
}
