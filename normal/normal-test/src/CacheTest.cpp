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
#include <normal/core/expression/Cast.h>
#include <normal/core/expression/Column.h>
#include <normal/core/type/Float64Type.h>
#include "Globals.h"

using namespace normal::core::type;
using namespace normal::core::expression;

/**
 * TODO: Throwing errors when issuing AWS requests, reported as a CRC error but suspect an auth issue. Skip for now.
 */
TEST_CASE ("CacheTest"
               * doctest::skip(false)) {
    std::string colList[] = {"l_quantity", "l_extendedprice", "l_discount", "L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY",
                             "L_LINENUMBER", "L_TAX"};
    int colIndexList[60];
    srand(time(NULL));

    for (int i = 0; i < 60; ++i) {
        colIndexList[i] = rand() % 8;
    }
    for (int k = 0; k < 9; ++k) {

    normal::pushdown::AWSClient client;
    client.init();

    char buff[FILENAME_MAX];
    getcwd(buff, FILENAME_MAX);
    std::string current_working_dir(buff);

    SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

    std::vector<std::string> cols;
    cols.emplace_back("l_extendedprice");
    auto s3selectScan1 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan1",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);
    auto s3selectScan2 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan2",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);
    auto s3selectScan3 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan3",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);
    auto s3selectScan4 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan4",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);
    auto s3selectScan5 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan5",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);
    auto s3selectScan6 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan6",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);
    auto s3selectScan7 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan7",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);
    auto s3selectScan8 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan8",
                                                                          "mit-caching",
                                                                          "test/a.tbl",
                                                                          "select l_extendedprice  from S3Object",
                                                                          "a",
                                                                          cols,
                                                                          client.defaultS3Client(),
                                                                          k);

    auto sumExpr1 = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));
    auto expressions1 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions1->push_back(sumExpr1);

    auto sumExpr2 = std::make_shared<normal::pushdown::aggregate::Sum>("sum",cast(col("f0"),float64Type()));
    auto expressions2 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions2->push_back(sumExpr2);

    auto sumExpr3 = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));
    auto expressions3 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions3->push_back(sumExpr3);

    auto sumExpr4 = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));
    auto expressions4 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions4->push_back(sumExpr4);

    auto sumExpr5 = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));
    auto expressions5 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions5->push_back(sumExpr5);

    auto sumExpr6 = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));
    auto expressions6 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions6->push_back(sumExpr6);

    auto sumExpr7 = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));
    auto expressions7 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions7->push_back(sumExpr7);

    auto sumExpr8 = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));
    auto expressions8 =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    expressions8->push_back(sumExpr8);

    auto aggregate1 = std::make_shared<normal::pushdown::Aggregate>("aggregate1", expressions1);
    auto aggregate2 = std::make_shared<normal::pushdown::Aggregate>("aggregate2", expressions2);
    auto aggregate3 = std::make_shared<normal::pushdown::Aggregate>("aggregate3", expressions3);
    auto aggregate4 = std::make_shared<normal::pushdown::Aggregate>("aggregate4", expressions4);
    auto aggregate5 = std::make_shared<normal::pushdown::Aggregate>("aggregate5", expressions5);
    auto aggregate6 = std::make_shared<normal::pushdown::Aggregate>("aggregate6", expressions6);
    auto aggregate7 = std::make_shared<normal::pushdown::Aggregate>("aggregate7", expressions7);
    auto aggregate8 = std::make_shared<normal::pushdown::Aggregate>("aggregate8", expressions8);

    auto reduceSumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum", col("sum"));
    auto
            reduceAggregateExpressions =
            std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    reduceAggregateExpressions->emplace_back(reduceSumExpr);
    auto reduceAggregate = std::make_shared<normal::pushdown::Aggregate>("reduceAggregate", reduceAggregateExpressions);

    auto collate = std::make_shared<normal::pushdown::Collate>("collate");

    s3selectScan1->produce(aggregate1);
    aggregate1->consume(s3selectScan1);

    s3selectScan2->produce(aggregate2);
    aggregate2->consume(s3selectScan2);

    s3selectScan3->produce(aggregate3);
    aggregate3->consume(s3selectScan3);

    s3selectScan4->produce(aggregate4);
    aggregate4->consume(s3selectScan4);

    s3selectScan5->produce(aggregate5);
    aggregate5->consume(s3selectScan5);

    s3selectScan6->produce(aggregate6);
    aggregate6->consume(s3selectScan6);

    s3selectScan7->produce(aggregate7);
    aggregate7->consume(s3selectScan7);

    s3selectScan8->produce(aggregate8);
    aggregate8->consume(s3selectScan8);

    aggregate1->produce(reduceAggregate);
    reduceAggregate->consume(aggregate1);

    aggregate2->produce(reduceAggregate);
    reduceAggregate->consume(aggregate2);

    aggregate3->produce(reduceAggregate);
    reduceAggregate->consume(aggregate3);

    aggregate4->produce(reduceAggregate);
    reduceAggregate->consume(aggregate4);

    aggregate5->produce(reduceAggregate);
    reduceAggregate->consume(aggregate5);

    aggregate6->produce(reduceAggregate);
    reduceAggregate->consume(aggregate6);

    aggregate7->produce(reduceAggregate);
    reduceAggregate->consume(aggregate7);

    aggregate8->produce(reduceAggregate);
    reduceAggregate->consume(aggregate8);

    reduceAggregate->produce(collate);
    collate->consume(reduceAggregate);

    auto mgr = std::make_shared<normal::core::OperatorManager>();

    mgr->put(s3selectScan1);
    mgr->put(s3selectScan2);
    mgr->put(s3selectScan3);
    mgr->put(s3selectScan4);
    mgr->put(s3selectScan5);
    mgr->put(s3selectScan6);
    mgr->put(s3selectScan7);
    mgr->put(s3selectScan8);
    mgr->put(aggregate1);
    mgr->put(aggregate2);
    mgr->put(aggregate3);
    mgr->put(aggregate4);
    mgr->put(aggregate5);
    mgr->put(aggregate6);
    mgr->put(aggregate7);
    mgr->put(aggregate8);
    mgr->put(reduceAggregate);
    mgr->put(collate);

//    mgr->boot();

//    mgr->start();
//    mgr->join();
//
//    auto tuples = collate->tuples();
//
//    auto val = std::stod(tuples->getValue("sum", 0));
//
//            CHECK(tuples->numRows() == 1);
//            CHECK(tuples->numColumns() == 1);
//    // CHECK(val == 4400227);
//
//    mgr->stop();
//    printf("again!");


    //run it again to test cache
//  auto mgr2 = std::make_shared<OperatorManager>();
////  auto s3selectScan2 = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan2",
////                                                                         "mit-caching",
////                                                                         "test/a.tbl",
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

//        colIndexList[0] = 7;
//        colIndexList[1] = 6;
//        colIndexList[2] = 1;
//        colIndexList[3] = 3;
//        colIndexList[4] = 1;
//        colIndexList[5] = 7;
//        colIndexList[6] = 2;
//        colIndexList[7] = 4;
//        colIndexList[8] = 1;
        std::ofstream outfile;

        outfile.open("testRes-FIFO-60.csv", std::ios_base::app); // append instead of overwrite
        mgr->boot();
        //cache every time
        auto start = std::chrono::system_clock::now();
        for (int i = 0; i < 60; ++i) {

            int colIndex = colIndexList[i];
            std::string colName = colList[colIndex];
            cols.clear();
            cols.emplace_back(colName);
            outfile << colName << ",";
            std::string query = "select " + colName + " from S3Object";
            s3selectScan1->setCols(cols);
            s3selectScan1->setQuery(query);
            s3selectScan2->setCols(cols);
            s3selectScan2->setQuery(query);
            s3selectScan3->setCols(cols);
            s3selectScan3->setQuery(query);
            s3selectScan4->setCols(cols);
            s3selectScan4->setQuery(query);
            s3selectScan5->setCols(cols);
            s3selectScan5->setQuery(query);
            s3selectScan6->setCols(cols);
            s3selectScan6->setQuery(query);
            s3selectScan7->setCols(cols);
            s3selectScan7->setQuery(query);
            s3selectScan8->setCols(cols);
            s3selectScan8->setQuery(query);
            auto startTime = std::chrono::system_clock::now();
            mgr->start();
            mgr->join();

            auto endTime = std::chrono::system_clock::now();
            auto elapsedTime = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);

            outfile << elapsedTime.count() << std::endl;
        }
        auto end = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        std::cout << elapsed.count() << '\n';
        mgr->stop();
        //push down every time
        start = std::chrono::system_clock::now();

        reduceSumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum", cast(col("f0"),float64Type()));

        reduceAggregateExpressions =
                std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
        reduceAggregateExpressions->emplace_back(reduceSumExpr);
        reduceAggregate = std::make_shared<normal::pushdown::Aggregate>("reduceAggregate", reduceAggregateExpressions);

        auto collate2 = std::make_shared<normal::pushdown::Collate>("collate2");

        s3selectScan1->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan1);

        s3selectScan2->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan2);

        s3selectScan3->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan3);

        s3selectScan4->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan4);

        s3selectScan5->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan5);

        s3selectScan6->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan6);

        s3selectScan7->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan7);

        s3selectScan8->produce(reduceAggregate);
        reduceAggregate->consume(s3selectScan8);

        reduceAggregate->produce(collate2);

        collate2->consume(reduceAggregate);
        auto mgr2 = std::make_shared<normal::core::OperatorManager>();

        mgr2->put(s3selectScan1);
        mgr2->put(s3selectScan2);
        mgr2->put(s3selectScan3);
        mgr2->put(s3selectScan4);
        mgr2->put(s3selectScan5);
        mgr2->put(s3selectScan6);
        mgr2->put(s3selectScan7);
        mgr2->put(s3selectScan8);
        mgr2->put(reduceAggregate);
        mgr2->put(collate2);

        mgr2->boot();
        for (int i = 0; i < 60; ++i) {

            int colIndex = colIndexList[i];
            std::string colName = colList[colIndex];
            outfile << colName << ",";
            cols.clear();
            cols.emplace_back(colName);
            std::string query = "select SUM(CAST(" + colName + " AS FLOAT)) from S3Object";
            s3selectScan1->setCols({"NA"});
            s3selectScan1->setQuery(query);
            s3selectScan2->setCols({"NA"});
            s3selectScan2->setQuery(query);
            s3selectScan3->setCols({"NA"});
            s3selectScan3->setQuery(query);
            s3selectScan4->setCols({"NA"});
            s3selectScan4->setQuery(query);
            s3selectScan5->setCols({"NA"});
            s3selectScan5->setQuery(query);
            s3selectScan6->setCols({"NA"});
            s3selectScan6->setQuery(query);
            s3selectScan7->setCols({"NA"});
            s3selectScan7->setQuery(query);
            s3selectScan8->setCols({"NA"});
            s3selectScan8->setQuery(query);
            auto startTime = std::chrono::system_clock::now();
            mgr2->start();
            mgr2->join();

//            tuples = collate2->tuples();
//
//            val = std::stod(tuples->getValue("sum", 0));


            auto endTime = std::chrono::system_clock::now();
            auto elapsedTime = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);

            outfile << elapsedTime.count() << std::endl;
        }
        mgr2->stop();
        end = std::chrono::system_clock::now();
        elapsed =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        std::cout << elapsed.count() << '\n';
        outfile.close();
        client.shutdown();
    }
}
