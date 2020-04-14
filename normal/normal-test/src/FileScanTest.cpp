//
// Created by matt on 5/3/20.
//

#include <string>
#include <memory>
#include <vector>
#include <cstdio>
#include <unistd.h>

#include <doctest/doctest.h>

#include "normal/pushdown/Collate.h"
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/FileScan.h>
#include <normal/pushdown/aggregate/Sum.h>
#include "Globals.h"
#include "normal/core/expression/Cast.h"
#include "normal/core/expression/Column.h"
#include "normal/core/type/Float64Type.h"
#include "TestUtil.h"

using namespace normal::core::type;
using namespace normal::core::expression;
using namespace normal::pushdown::aggregate;

TEST_CASE ("FileScan -> Sum -> Collate"
               * doctest::skip(false)) {

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto fileScan = std::make_shared<normal::pushdown::FileScan>("fileScan", "data/data-file-simple/test.csv");

  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  aggregateFunctions->emplace_back(std::make_shared<Sum>("Sum", cast(col("A"), float64Type())));

  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", aggregateFunctions);
  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  fileScan->produce(aggregate);
  aggregate->consume(fileScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  mgr->put(fileScan);
  mgr->put(aggregate);
  mgr->put(collate);

  TestUtil::writeLogicalExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = tuples->value<arrow::DoubleType>("Sum", 0);

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(val == 12.0);

  mgr->stop();
}

TEST_CASE ("Sharded FileScan -> Sum -> Collate"
               * doctest::skip(false)) {

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto fileScan01 = std::make_shared<normal::pushdown::FileScan>("fileScan01", "data/data-file-sharded/test01.csv");
  auto fileScan02 = std::make_shared<normal::pushdown::FileScan>("fileScan02", "data/data-file-sharded/test02.csv");
  auto fileScan03 = std::make_shared<normal::pushdown::FileScan>("fileScan03", "data/data-file-sharded/test03.csv");

  auto expressions01 = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  expressions01->emplace_back(std::make_shared<Sum>("sum(A)",
                                                    cast(col("A"), float64Type())));
  auto aggregate01 = std::make_shared<normal::pushdown::Aggregate>("aggregate01", expressions01);

  auto expressions02 = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  expressions02->emplace_back(std::make_shared<Sum>("sum(A)",
                                                    cast(col("A"), float64Type())));
  auto aggregate02 = std::make_shared<normal::pushdown::Aggregate>("aggregate02", expressions02);

  auto expressions03 = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  expressions03->emplace_back(std::make_shared<Sum>("sum(A)",
                                                    cast(col("A"), float64Type())));
  auto aggregate03 = std::make_shared<normal::pushdown::Aggregate>("aggregate03", expressions03);

  auto reduceAggregateExpressions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  reduceAggregateExpressions->emplace_back(std::make_shared<Sum>("sum(A)", col("sum(A)")));
  auto reduceAggregate = std::make_shared<normal::pushdown::Aggregate>("reduceAggregate", reduceAggregateExpressions);

  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  fileScan01->produce(aggregate01);
  aggregate01->consume(fileScan01);

  fileScan02->produce(aggregate02);
  aggregate02->consume(fileScan02);

  fileScan03->produce(aggregate03);
  aggregate03->consume(fileScan03);

  aggregate01->produce(reduceAggregate);
  reduceAggregate->consume(aggregate01);

  aggregate02->produce(reduceAggregate);
  reduceAggregate->consume(aggregate02);

  aggregate03->produce(reduceAggregate);
  reduceAggregate->consume(aggregate03);

  reduceAggregate->produce(collate);
  collate->consume(reduceAggregate);

  mgr->put(fileScan01);
  mgr->put(fileScan02);
  mgr->put(fileScan03);
  mgr->put(aggregate01);
  mgr->put(aggregate02);
  mgr->put(aggregate03);
  mgr->put(reduceAggregate);
  mgr->put(collate);

  TestUtil::writeLogicalExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = tuples->value<arrow::DoubleType>("sum(A)", 0);

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(val == 36.0);

  mgr->stop();

}