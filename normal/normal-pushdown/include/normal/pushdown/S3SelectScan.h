//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H

#include <memory>
#include <string>

#include <aws/s3/S3Client.h>
#include <normal/core/Cache.h>

#include "normal/core/Operator.h"
#include "normal/core/TupleSet.h"


namespace normal::pushdown {

class S3SelectScan : public normal::core::Operator {
private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::string sql_;
  std::shared_ptr<Cache> m_cache;
  std::vector<std::string> m_col;
  std::string m_tbl;
  std::shared_ptr<Aws::S3::S3Client> s3Client_;

  void onStart();


public:
  S3SelectScan(std::string name, std::string s3Bucket, std::string s3Object, std::string sql, std::string m_tbl, std::vector<std::string> m_col, std::shared_ptr<Aws::S3::S3Client> s3Client);
  ~S3SelectScan() override = default;
  std::shared_ptr<Cache> getCache(){
      return m_cache;
  }
  void onReceive(const core::Envelope &message) override;
  //check if caching is needed or not
  bool checkCache();
  void setCols(std::vector<std::string> newcols){
      m_col = newcols;
  }
  void setQuery(std::string sql){
      sql_ = sql;
  }

};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
