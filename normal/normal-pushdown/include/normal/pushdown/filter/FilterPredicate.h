//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTERPREDICATE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTERPREDICATE_H

#include <memory>
#include <caf/all.hpp>
#include <normal/util/CAFUtil.h>
#include <normal/expression/gandiva/Expression.h>

namespace normal::pushdown::filter {

class FilterPredicate {

public:
  explicit FilterPredicate(std::shared_ptr<normal::expression::gandiva::Expression> Expr);
  FilterPredicate() = default;
  FilterPredicate(const FilterPredicate&) = default;
  FilterPredicate& operator=(const FilterPredicate&) = default;

  static std::shared_ptr<FilterPredicate> make(const std::shared_ptr<normal::expression::gandiva::Expression>& Expr);

  const std::shared_ptr<normal::expression::gandiva::Expression> &expression() const;

private:
  std::shared_ptr<normal::expression::gandiva::Expression> expr_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FilterPredicate& op) {
    return f.apply(op.expr_);
  }
};

}

using FilterPredicatePtr = std::shared_ptr<normal::pushdown::filter::FilterPredicate>;

CAF_BEGIN_TYPE_ID_BLOCK(FilterPredicate, normal::util::FilterPredicate_first_custom_type_id)
CAF_ADD_TYPE_ID(FilterPredicate, (normal::pushdown::filter::FilterPredicate))
CAF_END_TYPE_ID_BLOCK(FilterPredicate)

namespace caf {
template <>
struct inspector_access<FilterPredicatePtr> : variant_inspector_access<FilterPredicatePtr> {
    // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTERPREDICATE_H
