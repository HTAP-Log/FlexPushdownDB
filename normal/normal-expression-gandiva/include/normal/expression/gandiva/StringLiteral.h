//
// Created by Yifei Yang on 7/15/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_STRINGLITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_STRINGLITERAL_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include "Expression.h"

namespace normal::expression::gandiva {

class StringLiteral : public Expression {

public:
  explicit StringLiteral(std::string value);
  StringLiteral() = default;
  StringLiteral(const StringLiteral&) = default;
  StringLiteral& operator=(const StringLiteral&) = default;

  void compile(std::shared_ptr<arrow::Schema>) override;

  std::string alias() override;

  std::shared_ptr<std::vector<std::string> > involvedColumnNames() override;

  const std::string &value() const;

private:
  std::string value_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StringLiteral& exp) {
    return f.object(exp).fields(f.field("value", exp.value_),
                                f.field("expType", exp.expType_));
  }
};

std::shared_ptr<Expression> str_lit(std::string value);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_STRINGLITERAL_H
