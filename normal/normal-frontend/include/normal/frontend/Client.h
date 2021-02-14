//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_FRONTEND_CLIENT_H
#define NORMAL_FRONTEND_CLIENT_H

#include <normal/frontend/Global.h>
#include <normal/cache/CachingPolicy.h>
#include <normal/plan/mode/Modes.h>
#include <normal/sql/Interpreter.h>

using namespace normal::cache;
using namespace normal::plan::operator_::mode;
using namespace normal::sql;

namespace normal::frontend {

class Client {
public:
  explicit Client();
  std::string boot();
  std::string stop();
  std::string reboot();
  std::string setCachingPolicy(int cachingPolicyType);
  std::string setMode(int modeType);
  std::string executeSql(const std::string &sql);
  std::string executeSqlFile(const std::string &filePath);

private:
  size_t cacheSize_;
  std::string bucketName_;
  std::string dirPrefix_;
  std::shared_ptr<CachingPolicy> cachingPolicy_;
  std::shared_ptr<Mode> mode_;
  std::shared_ptr<Interpreter> interpreter_;

  void configureS3ConnectorSinglePartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix);
  void configureS3ConnectorMultiPartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix);
  std::shared_ptr<TupleSet> execute();
};

}


#endif //NORMAL_FRONTEND_CLIENT_H
