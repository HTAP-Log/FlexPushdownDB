//
// Created by Yifei Yang on 3/2/21.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_TUPLESETSER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_TUPLESETSER_H

#include <arrow/api.h>

namespace normal::tuple {

  std::shared_ptr<arrow::Table> bytes_to_table(const std::vector<std::uint8_t>& bytes_vec);
  std::vector<std::uint8_t> table_to_bytes(const std::shared_ptr<arrow::Table>& table);
}


#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_TUPLESETSER_H
