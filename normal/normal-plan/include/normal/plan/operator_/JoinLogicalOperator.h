//
// Created by Yifei Yang on 7/14/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_JOINLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_JOINLOGICALOPERATOR_H

#include <string>
#include <normal/expression/gandiva/Expression.h>
#include <normal/plan/operator_/LogicalOperator.h>

namespace normal::plan::operator_ {

class JoinLogicalOperator : public LogicalOperator{

public:
  explicit JoinLogicalOperator(const std::string &leftColumnName, const std::string &rightColumnName,
          const std::shared_ptr<LogicalOperator> &leftProducer,
          const std::shared_ptr<LogicalOperator> &rightProducer);

  std::vector<std::pair<std::shared_ptr<core::Operator>, int> > toOperatorsWithPlacementsUniHash(int numNodes) override;

  const std::string &getLeftColumnName() const;
  const std::string &getRightColumnName() const;

  const std::shared_ptr<LogicalOperator> &getLeftProducer() const;
  const std::shared_ptr<LogicalOperator> &getRightProducer() const;

  void setNeededColumnNames(const std::set<std::string> &neededColumnNames);
  const std::set<std::string> &getNeededColumnNames() const;

private:
  std::string leftColumnName_;
  std::string rightColumnName_;

  std::shared_ptr<LogicalOperator> leftProducer_;
  std::shared_ptr<LogicalOperator> rightProducer_;

  // columns needed by downstream operators
  std::set<std::string> neededColumnNames_;
};

}


#endif //NORMAL_JOINLOGICALOPERATOR_H
