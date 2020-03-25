//
// Created by matt on 7/3/20.
//

#include <string>
#include <memory>
#include <vector>
#include <cstdio>
#include <unistd.h>

#include <doctest/doctest.h>

#include "normal/pushdown/S3SelectScan.h"
#include "normal/pushdown/Collate.h"
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/pushdown/aggregate/Sum.h>
#include "Globals.h"

/**
 * TODO: Throwing errors when issuing AWS requests, reported as a CRC error but suspect an auth issue. Skip for now.
 */
TEST_CASE ("CacheTest"
               * doctest::skip(false)) {

  normal::pushdown::AWSClient client;
  client.init();

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

  std::vector<std::string> cols;
  cols.emplace_back("l_extendedprice");
  auto s3selectScan = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan",
                                                                       "mit-caching",
                                                                       "lineitemwithH.tbl",
                                                                       "select l_extendedprice  from S3Object",
                                                                       "a",
                                                                       cols,
                                                                       client.defaultS3Client());
  auto cache = s3selectScan->getCache();

  auto sumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum", "f0");
  auto expressions2 =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions2->push_back(sumExpr);

  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", expressions2);

  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  s3selectScan->produce(aggregate);
  aggregate->consume(s3selectScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  auto mgr = std::make_shared<OperatorManager>();

  mgr->put(s3selectScan);
  mgr->put(aggregate);
  mgr->put(collate);

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = std::stod(tuples->getValue("sum", 0));

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
     // CHECK(val == 4400227);

  mgr->stop();
  printf("again!");


  //run it again to test cache
//  auto mgr2 = std::make_shared<OperatorManager>();
////  auto s3selectScan2 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan2",
////                                                                         "mit-caching",
////                                                                         "lineitemwithH.tbl",
////                                                                         "select  * from S3Object",
////                                                                         "a",
////                                                                         "all",
////                                                                         client.defaultS3Client());
////  auto aggregate2 = std::make_shared<normal::pushdown::Aggregate>("aggregate", expressions2);
////
////  auto collate2 = std::make_shared<normal::pushdown::Collate>("collate");
//
//  mgr2->put(s3selectScan);
//  mgr2->put(aggregate);
//  mgr2->put(collate);
  std::string colList[] = {"l_quantity","l_extendedprice","l_discount","L_ORDERKEY","L_PARTKEY","L_SUPPKEY","L_LINENUMBER","L_TAX"};
  int colIndexList[60];
  for (int i=0; i<60; ++i) {
    colIndexList[i] = rand() % 8;
  }
  colIndexList[0] = 7;
    colIndexList[1] = 6;
    colIndexList[2] = 1;
    colIndexList[3] = 3;
    colIndexList[4] = 1;
    colIndexList[5] = 7;
    colIndexList[6] = 2;
    colIndexList[7] = 4;
    colIndexList[8] = 1;
  std::ofstream outfile;

  outfile.open("testRes-FIFO-60.csv", std::ios_base::app); // append instead of overwrite

   //cache every time
  auto start = std::chrono::system_clock::now();
  for (int i=0; i<60; ++i) {
      auto startTime = std::chrono::system_clock::now();
      int colIndex  = colIndexList[i];
      std::string colName = colList[colIndex];
      cols.clear();
      cols.emplace_back(colName);
      outfile << colName << ",";
      std::string query = "select " +colName+ " from S3Object";
      s3selectScan->setCols(cols);
      s3selectScan->setQuery(query);
      mgr->start();
      mgr->join();
      mgr->stop();
      auto endTime = std::chrono::system_clock::now();
      auto elapsedTime  = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);

      outfile << elapsedTime.count() << std::endl;
  }
    auto end = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << elapsed.count() << '\n';
  //push down every time
    start = std::chrono::system_clock::now();
    auto collate2 = std::make_shared<normal::pushdown::Collate>("collate");
    s3selectScan->produce(collate2);

    collate2->consume(s3selectScan);
    auto mgr2 = std::make_shared<OperatorManager>();

    mgr2->put(s3selectScan);

    mgr2->put(collate2);


    for (int i=0; i<60; ++i) {
        auto startTime = std::chrono::system_clock::now();
        int colIndex  = colIndexList[i];
        std::string colName = colList[colIndex];
        outfile << colName << ",";
        cols.clear();
        cols.emplace_back(colName);
        std::string query = "select SUM(CAST(" +colName+ " AS FLOAT)) from S3Object";
        s3selectScan->setCols({"NA"});
        s3selectScan->setQuery(query);
        mgr2->start();
        mgr2->join();

        tuples = collate2->tuples();

        val = std::stod(tuples->getValue("f0", 0));


        mgr->stop();
        auto endTime = std::chrono::system_clock::now();
        auto elapsedTime  = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);

        outfile << elapsedTime.count() << std::endl;
    }
    end = std::chrono::system_clock::now();
    elapsed =
            std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << elapsed.count() << '\n';
    outfile.close();
  client.shutdown();
}
