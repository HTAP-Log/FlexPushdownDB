//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLESET_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLESET_H

#include <memory>
#include <vector>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <tl/expected.hpp>
#include <normal/tuple/arrow/Arrays.h>
#include <normal/tuple/arrow/TableHelper.h>
#include "normal/tuple/Globals.h"

namespace arrow { class Table; }
namespace arrow::csv { class TableReader; }

namespace normal::tuple {

/**
 * A list of tuples/rows/records. Really just encapsulates Arrow tables and record batches. Hiding
 * some of the rough edges.
 */
class TupleSet {

private:
  std::shared_ptr<arrow::Table> table_;

public:
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::shared_ptr<arrow::Array>>& values){
    auto tuples = std::make_shared<TupleSet>();
    tuples->table_ = arrow::Table::Make(schema, values);
    return tuples;
  }

  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::shared_ptr<arrow::ChunkedArray>>& values){
    auto tuples = std::make_shared<TupleSet>();
    tuples->table_ = arrow::Table::Make(schema, values);
    return tuples;
  }

  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::csv::TableReader> &tableReader);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Table> &table);
  static std::shared_ptr<TupleSet> concatenate(const std::shared_ptr<TupleSet>&,const std::shared_ptr<TupleSet>&);
  int64_t numRows();
  std::shared_ptr<arrow::Scalar> visit(const std::function<std::shared_ptr<arrow::Scalar>(std::shared_ptr<arrow::Scalar>, arrow::RecordBatch &)>& fn);
//  void addColumn(const std::string &name, int position, std::vector<std::string> data);

  std::string toString();

  [[nodiscard]] std::shared_ptr<arrow::Table> table() const;
  void table(const std::shared_ptr<arrow::Table> &table);

  [[deprecated ("getValue is a hack that returns everything as a string, use value()")]] std::string getValue(const std::string &columnName, int row);
  int64_t numColumns();

  /**
   * Returns an element from the tupleset given and column name and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  tl::expected<C_TYPE, std::string> value(const std::string &columnName, int row){
    return TableHelper::value<ARROW_TYPE, C_TYPE>(columnName, row, *table_);
  }

  tl::expected<std::string, std::string> getString(const std::string &columnName, int row){
	return TableHelper::value<::arrow::StringType, std::string>(columnName, row, *table_);
  }


  /**
   * Returns an element from the tupleset given and column number and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  tl::expected<C_TYPE, std::string> value(int column, int row){
	return TableHelper::value<ARROW_TYPE, C_TYPE>(column, row, *table_);
  }

  /**
   * Returns a column from the tupleset as a vector given a column name
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  tl::expected<std::shared_ptr<std::vector<C_TYPE>>, std::string> vector(const std::string &columnName){
	return TableHelper::vector<ARROW_TYPE, C_TYPE>(*table_->GetColumnByName(columnName));
  }

  /**
   * Templated visitor, visits record batches from the internal arrow table and applies the given
   * function to each batch. Returns the result of the function.
   *
   * FIXME: On;y returning last result
   *
   * @tparam T
   * @param fn
   * @return
   */
//  template <typename T>
//  T visit2(const std::function<T(arrow::RecordBatch &)> &fn) {
//
//    arrow::Status arrowStatus;
//
//    std::shared_ptr<arrow::RecordBatch> batch;
//    arrow::TableBatchReader reader(*table_);
//
//    reader.set_chunksize(tuple::DefaultChunkSize);
//    arrowStatus = reader.ReadNext(&batch);
//
//    T result;
//    while (arrowStatus.ok() && batch) {
//      result = fn(result, *batch);
//      arrowStatus = reader.ReadNext(&batch);
//    }
//
//    return result;
//  }

//  tl::expected<std::shared_ptr<TupleSet>, std::string>
//  evaluate(const std::shared_ptr<normal::expression::Projector> &projector);
};

}

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLESET_H
