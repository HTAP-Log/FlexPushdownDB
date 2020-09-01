//
// Created by Yifei Yang on 8/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H

#include <normal/ssb/Globals.h>
#include <map>
#include <random>

namespace normal::ssb {

enum QueryName {
    Query1_1, Query1_2, Query1_3,
    Query2_1, Query2_2, Query2_3,
    Query3_1, Query3_2, Query3_3, Query3_4,
    Query4_1, Query4_2, Query4_3,
    Unknown
};

enum SkewQueryName {
    SkewQuery2_1, SkewQuery2_2, SkewQuery2_3,
    SkewQuery3_1, SkewQuery3_2, SkewQuery3_3, SkewQuery3_4,
    SkewQuery4_1, SkewQuery4_2, SkewQuery4_3,
    SkewUnknown
};

class SqlGenerator {

public:
  SqlGenerator();
  std::vector<std::string> generateSqlBatch(int batchSize);
  std::string generateSql(std::string queryName);

  /**
   * The following is used to generate skew benchmark
   */
  std::vector<std::string> generateSqlBatchSkew(int batchSize);
  std::string generateSqlSkew(std::string queryName, std::string lo_predicate);

private:
  std::shared_ptr<std::default_random_engine> generator_;
  std::map<std::string, QueryName> queryNameMap_;
  std::map<std::string, SkewQueryName> skewQueryNameMap_;

  std::string genQuery1_1();
  std::string genQuery1_2();
  std::string genQuery1_3();
  std::string genQuery2_1();
  std::string genQuery2_2();
  std::string genQuery2_3();
  std::string genQuery3_1();
  std::string genQuery3_2();
  std::string genQuery3_3();
  std::string genQuery3_4();
  std::string genQuery4_1();
  std::string genQuery4_2();
  std::string genQuery4_3();

  int genD_year();
  int genD_yearmonthnum();
  int genD_weeknuminyear();
  std::string genD_yearmonth();
  int genLo_discount();
  int genLo_quantity();
  std::string genLo_predicate();
  int genP_category_num();
  int genP_brand1_num();
  int genP_mfgr_num();
  std::string genS_region();
  std::string genS_nation();
  std::string genS_city();

  /**
   * The following is used to generate skew benchmark
   */
  std::string genSkewQuery2_1(std::string skewLo_predicate);
  std::string genSkewQuery2_2(std::string skewLo_predicate);
  std::string genSkewQuery2_3(std::string skewLo_predicate);
  std::string genSkewQuery3_1(std::string skewLo_predicate);
  std::string genSkewQuery3_2(std::string skewLo_predicate);
  std::string genSkewQuery3_3(std::string skewLo_predicate);
  std::string genSkewQuery3_4(std::string skewLo_predicate);
  std::string genSkewQuery4_1(std::string skewLo_predicate);
  std::string genSkewQuery4_2(std::string skewLo_predicate);
  std::string genSkewQuery4_3(std::string skewLo_predicate);
};

}


#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H
