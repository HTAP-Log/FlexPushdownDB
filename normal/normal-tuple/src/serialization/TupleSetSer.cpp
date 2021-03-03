//
// Created by Yifei Yang on 3/2/21.
//

#include <normal/tuple/serialization/TupleSetSer.h>
#include <fmt/format.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

std::shared_ptr<arrow::Table> normal::tuple::bytes_to_table(const std::vector<std::uint8_t>& bytes_vec) {
  arrow::Status status;

  // Create a view over the given byte vector, but then get a copy because the vector ref eventually disappears
  auto buffer_view = ::arrow::Buffer::Wrap(bytes_vec);
  auto maybe_buffer = buffer_view->CopySlice(0, buffer_view->size(), arrow::default_memory_pool());
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", maybe_buffer.status().message()));

  // Get a reader over the buffer
  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(*maybe_buffer);

  // Get a record batch reader over that
  auto maybe_reader = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
  if (!maybe_reader.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", maybe_reader.status().message()));

  // Read the table
  std::shared_ptr<arrow::Table> table;
  status = (*maybe_reader)->ReadAll(&table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting bytes to Arrow table  |  error: {}", status.message()));

  return table;
}

std::vector<std::uint8_t> normal::tuple::table_to_bytes(const std::shared_ptr<arrow::Table>& table) {
  arrow::Status status;

  auto maybe_output_stream = arrow::io::BufferOutputStream::Create();
  if (!maybe_output_stream.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_output_stream.status().message()));

  auto maybe_writer = arrow::ipc::MakeStreamWriter((*maybe_output_stream).get(), table->schema());
  if (!maybe_writer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", maybe_writer.status().message()));

  status = (*maybe_writer)->WriteTable(*table);
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  status = (*maybe_writer)->Close();
  if (!status.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  auto maybe_buffer = (*maybe_output_stream)->Finish();
  if (!maybe_buffer.ok())
    throw std::runtime_error(fmt::format("Error converting Arrow table to bytes  |  error: {}", status.message()));

  auto data = (*maybe_buffer)->data();
  auto length = (*maybe_buffer)->size();

  std::vector<std::uint8_t> bytes_vec(data, data + length);

  return bytes_vec;
}
