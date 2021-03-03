//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_COLUMN_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_COLUMN_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"

namespace normal::expression::gandiva {

class Column : public Expression {

public:
  explicit Column(std::string columnName);
  Column() = default;
  Column(const Column&) = default;
  Column& operator=(const Column&) = default;

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

  std::shared_ptr<std::vector<std::string> > involvedColumnNames() override;

  const std::string &getColumnName() const;

private:
  std::string columnName_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Column& exp) {
    return f.object(exp).fields(f.field("columnName", exp.columnName_),
                                f.field("expType", exp.expType_));
  }
};

std::shared_ptr<Expression> col(const std::string& columnName);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_COLUMN_H
