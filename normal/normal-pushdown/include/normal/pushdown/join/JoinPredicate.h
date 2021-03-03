//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H

#include <string>

namespace normal::pushdown::join {

/**
 * A join predicate for straight column to column joins
 *
 * TODO: Support expressions
 */
class JoinPredicate {
  
public:
  JoinPredicate(std::string leftColumnName, std::string rightColumnName);
  JoinPredicate() = default;
  JoinPredicate(const JoinPredicate&) = default;
  JoinPredicate& operator=(const JoinPredicate&) = default;
  const std::string &getLeftColumnName() const;
  const std::string &getRightColumnName() const;
  static JoinPredicate create(const std::string &leftColumnName, const std::string &rightColumnName);

private:
  std::string leftColumnName_;
  std::string rightColumnName_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, JoinPredicate& pred) {
    return f.object(pred).fields(f.field("leftColumnName", pred.leftColumnName_),
                                 f.field("rightColumnName", pred.rightColumnName_));
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H
