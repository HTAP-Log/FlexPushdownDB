//
// Created by Yifei Yang on 3/2/21.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_ARROWSER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_ARROWSER_H

#include <arrow/api.h>

namespace normal::tuple {

  /**
   * Serialization and deserialization methods of Table.
   */
  std::shared_ptr<arrow::Table> bytes_to_table(const std::vector<std::uint8_t>& bytes_vec);
  std::vector<std::uint8_t> table_to_bytes(const std::shared_ptr<arrow::Table>& table);

  /**
   * Serialization and deserialization methods of ChunkedArray.
   * As the primitive serialization unit of Arrow is RecordBatch, here we do indirectly by making a Table first
   */
  std::shared_ptr<arrow::ChunkedArray> bytes_to_chunkedArray(const std::vector<std::uint8_t>& bytes_vec);
  std::vector<std::uint8_t> chunkedArray_to_bytes(const std::shared_ptr<arrow::ChunkedArray>& chunkedArray);
}


#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_ARROWSER_H
