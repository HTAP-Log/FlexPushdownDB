//
// Created by Yifei Yang on 10/10/20.
//

#ifndef NORMAL_TESTS_H
#define NORMAL_TESTS_H

#include <cstddef>

namespace normal::ssb {
  void mainTest(size_t cacheSize, int modeType, int cachingPolicyType, std::string dirPrefix,
                size_t networkLimit, bool writeResults);
  void htapTest(std::string col_name, std::string row_name);
  void perfBatchRun(int modeType, std::string dirPrefix, int cacheLoadQueries,
                    int warmupQueriesPerColSize, int columnSizesToTest, int rowSelectivityValuesToTest);
  void concurrentSelectTest(int numRequests);
  void concurrentGetTest(int numRequests);
}

#endif //NORMAL_TESTS_H
