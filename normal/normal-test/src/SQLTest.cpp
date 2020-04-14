//
// Created by matt on 5/3/20.
//

#include <experimental/filesystem>

#include <doctest/doctest.h>

#include <Interpreter.h>
#include <connector/s3/S3SelectConnector.h>
#include <connector/Catalogue.h>
#include <connector/s3/S3SelectCatalogueEntry.h>
#include <connector/local-fs/LocalFileSystemConnector.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/pushdown/Collate.h>
#include "Globals.h"
#include "TestUtil.h"

void configureLocalConnector(Interpreter &i) {
  auto conn = std::make_shared<LocalFileSystemConnector>("local_fs");
  auto cat = std::make_shared<Catalogue>("local_fs", conn);
  cat->put(std::make_shared<LocalFileSystemCatalogueEntry>("test", "data/data-file-simple/test.csv", cat));
  i.put(cat);
}

void configureS3Connector(Interpreter &i) {
  auto conn = std::make_shared<S3SelectConnector>("s3_select");
  auto cat = std::make_shared<Catalogue>("s3_select", conn);
  cat->put(std::make_shared<S3SelectCatalogueEntry>("customer", "s3Filter", "tpch-sf1/customer.csv", cat));
  i.put(cat);
}

auto execute(Interpreter &i) {
  i.getOperatorManager()->boot();
  i.getOperatorManager()->start();
  i.getOperatorManager()->join();

  std::shared_ptr<normal::pushdown::Collate>
      collate = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorManager()->getOperator("collate"));

  auto tuples = collate->tuples();

  SPDLOG_DEBUG(tuples->toString());

  return tuples;
}

auto executeTest(const std::string &sql) {
  Interpreter i;
  configureLocalConnector(i);
  configureS3Connector(i);
  i.parse(sql);
  TestUtil::writeLogicalExecutionPlan(*i.getOperatorManager());
  auto tuples = execute(i);
  i.getOperatorManager()->stop();

  return tuples;
}

//TEST_CASE ("sql-select-sum_a-from-s3"
//               * doctest::skip(false)) {
//  auto tuples = executeTest("select * from s3_select.customer");
//
//}

TEST_CASE ("sql-select-sum_a-from-local"
               * doctest::skip(false)) {
  auto tuples = executeTest("select sum(cast(A as double)) from local_fs.test");
      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(tuples->value<arrow::DoubleType, double>("sum", 0) == 12.0);
}

TEST_CASE ("sql-select-all-from-local" * doctest::skip(false)) {
  auto tuples = executeTest("select * from local_fs.test");
      CHECK(tuples->numRows() == 3);
      CHECK(tuples->numColumns() == 3);
      CHECK(tuples->value<arrow::Int64Type, int>("A", 0) == 1.0);
}

//TEST_CASE ("sql-select-cast_a-from-local" * doctest::skip(false)) {
//  auto tuples = executeTest("select cast(A as double) from local_fs.test");
//      CHECK(tuples->numRows() == 3);
//      CHECK(tuples->numColumns() == 3);
//      CHECK(tuples->value<arrow::DoubleType>("A", 0) == 1.0);
//}
