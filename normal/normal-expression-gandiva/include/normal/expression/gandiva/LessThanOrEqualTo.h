//
// Created by matt on 11/6/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LESSTHANOREQUALTO_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LESSTHANOREQUALTO_H


#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class LessThanOrEqualTo : public BinaryExpression {

public:
  LessThanOrEqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);
  LessThanOrEqualTo() = default;
  LessThanOrEqualTo(const LessThanOrEqualTo&) = default;
  LessThanOrEqualTo& operator=(const LessThanOrEqualTo&) = default;

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LessThanOrEqualTo& exp) {
    return f.object(exp).fields(f.field("left", exp.left_),
                                f.field("right", exp.right_),
                                f.field("expType", exp.expType_));
  }
};

std::shared_ptr<Expression> lte(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LESSTHANOREQUALTO_H
