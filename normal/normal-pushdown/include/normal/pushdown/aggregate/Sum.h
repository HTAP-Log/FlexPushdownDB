//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H

#include <normal/pushdown/TupleMessage.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/gandiva/serialization/ExpressionSer.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>


namespace normal::pushdown::aggregate {

class Sum : public AggregationFunction {

private:
  std::shared_ptr<normal::expression::gandiva::Expression> expression_;
  constexpr static const char *const SUM_RESULT_KEY = "SUM";

public:
  Sum(std::string columnName, std::shared_ptr<normal::expression::gandiva::Expression> expression);
  Sum() = default;
  Sum(const Sum&) = default;
  Sum& operator=(const Sum&) = default;
  ~Sum() override = default;

  void apply(std::shared_ptr<aggregate::AggregationResult> result, std::shared_ptr<TupleSet> tuples) override;
  std::shared_ptr<arrow::DataType> returnType() override;

  void finalize(std::shared_ptr<aggregate::AggregationResult> result) override;

  void cacheInputSchema(const TupleSet &tuples);
  void buildAndCacheProjector();

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Sum& func) {
    return f.object(func).fields(f.field("expression", func.expression_),
                                 f.field("alias", func.alias()),
                                 f.field("type", func.type()));
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H
