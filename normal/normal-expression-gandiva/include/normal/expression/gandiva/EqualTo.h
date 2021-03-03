//
// Created by matt on 11/6/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EQUALTO_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EQUALTO_H


#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class EqualTo : public BinaryExpression {

public:
  EqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);
  EqualTo() = default;
  EqualTo(const EqualTo&) = default;
  EqualTo& operator=(const EqualTo&) = default;

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, EqualTo& exp) {
    return f.object(exp).fields(f.field("left", exp.left_),
                                f.field("right", exp.right_),
                                f.field("expType", exp.expType_));
  }
};

std::shared_ptr<Expression> eq(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EQUALTO_H
