//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/frontend/Client.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/cache/FBRSCachingPolicy.h>
#include <normal/cache/WFBRCachingPolicy.h>
#include <normal/plan/Globals.h>
#include <normal/util/Util.h>

using namespace normal::frontend;
using namespace normal::connector;
using namespace normal::sql;
using namespace normal::util;

Client::Client():
  cacheSize_(DefaultCacheSize_), bucketName_(DefaultBucketName_), dirPrefix_(DefaultDirPrefix_),
  distributed_(true) {
  ::caf::init_global_meta_objects<::caf::id_block::Client>();
  normal::core::init_caf_global_meta_objects();
  clientActorSystem_ = std::make_shared<caf::actor_system>(clientCfg_);
  serverActorSystem_ = std::make_shared<caf::actor_system>(serverCfg_);
}

std::string Client::boot() {
  /* Initialize query processing */
  defaultMiniCatalogue = MiniCatalogue::defaultMiniCatalogue(bucketName_, dirPrefix_);
  setMode(DefaultModeType_);
  setCachingPolicy(DefaultCachingPolicyType_);
  interpreter_ = std::make_shared<Interpreter>(mode_, cachingPolicy_);
  configureS3ConnectorMultiPartition(interpreter_, bucketName_, dirPrefix_);
  interpreter_->boot(clientActorSystem_);
  std::string output = "Client booted";

  /* Initialize remote communication */
  server();
  output += "; server started";
  return output;
}

std::string Client::stop() {
  interpreter_->getOperatorGraph().reset();
  interpreter_->stop();
  interpreter_.reset();
  defaultMiniCatalogue.reset();
  cachingPolicy_.reset();
  mode_.reset();
  return "Client stopped";
}

std::string Client::reboot() {
  stop();
  boot();
  return "Client rebooted";
}

void Client::connect() {
  auto expectedNode = clientActorSystem_->middleman().connect(clientCfg_.host_, clientCfg_.port_);
  if (!expectedNode) {
    std::cerr << "*** connect failed: " << to_string(expectedNode.error()) << std::endl;
    return;
  }
  std::cout << "connected to server" << std::endl;
  node_ = expectedNode.value();
}

void Client::server() {
  auto res = serverActorSystem_->middleman().open(serverCfg_.port_);
  if (!res) {
    std::cerr << "*** cannot open port: " << to_string(res.error()) << std::endl;
    return;
  }
}

void Client::setDistributed(bool distributed) {
  distributed_ = distributed;
}

std::shared_ptr<TupleSet> Client::execute() {
  interpreter_->getCachingPolicy()->onNewQuery();
  if (distributed_ && node_.has_value()) {
    interpreter_->getOperatorGraph()->setNode(node_.value());
  }
  interpreter_->getOperatorGraph()->boot();
  interpreter_->getOperatorGraph()->start();
  interpreter_->getOperatorGraph()->join();
  auto tuples = interpreter_->getOperatorGraph()->getLegacyCollateOperator()->tuples();
  return tuples;
}

std::string Client::executeSql(const std::string &sql) {
  interpreter_->clearOperatorGraph();
  interpreter_->parse(sql);
  if (distributed_ && node_.has_value()) {
    interpreter_->plan(2);
  } else {
    interpreter_->plan(1);
  }
  auto tuples = execute();
  auto tupleSet = TupleSet2::create(tuples);
  std::string output = fmt::format("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  output += fmt::format("\nMetrics:\n{}", interpreter_->getOperatorGraph()->showMetrics());
  output += fmt::format("\nFinished, time: {} secs", (double) (interpreter_->getOperatorGraph()->getElapsedTime().value()) / 1000000000.0);
  return output;
}

std::string Client::executeSqlFile(const std::string &filePath) {
  // Default sql files put under cmake.../normal-ssb/sql
  auto currentPath = filesystem::current_path().parent_path().append("normal-ssb/sql");
  auto sql_file_dir_path = currentPath.append(filePath);
  auto sql = readFile(sql_file_dir_path.string());
  return executeSql(sql);
}

std::string Client::setCachingPolicy(int cachingPolicyType) {
  std::string response = "Caching policy set to: ";
  switch (cachingPolicyType) {
    case 1: cachingPolicy_ = LRUCachingPolicy::make(cacheSize_, mode_); break;
    case 2: cachingPolicy_ = FBRSCachingPolicy::make(cacheSize_, mode_); break;
    case 3: cachingPolicy_ = WFBRCachingPolicy::make(cacheSize_, mode_); break;
    default: response = "Caching policy not found, unchanged"; return response;
  }
  return response + cachingPolicy_->toString();
}

std::string Client::setMode(int modeType) {
  std::string response = "Mode set to: ";
  switch (modeType) {
    case 1: mode_ = normal::plan::operator_::mode::Modes::fullPullupMode(); break;
    case 2: mode_ = normal::plan::operator_::mode::Modes::fullPushdownMode(); break;
    case 3: mode_ = normal::plan::operator_::mode::Modes::pullupCachingMode(); break;
    case 4: mode_ = normal::plan::operator_::mode::Modes::hybridCachingMode(); break;
    default: response = "Mode not found, unchanged"; return response;
  }
  return response + mode_->toString();
}

void Client::configureS3ConnectorSinglePartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // look up tables
  auto tableNames = normal::connector::defaultMiniCatalogue->tables();
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (const auto &tableName: *tableNames) {
    auto s3Object = dir_prefix + tableName + ".tbl";
    s3Objects->emplace_back(s3Object);
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, normal::plan::DefaultS3Client);

  // configure s3Connector
  for (int tbl_id = 0; tbl_id < tableNames->size(); tbl_id++) {
    auto &tableName = tableNames->at(tbl_id);
    auto &s3Object = s3Objects->at(tbl_id);
    auto numBytes = objectNumBytes_Map.find(s3Object)->second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i->put(cat);
}

void Client::configureS3ConnectorMultiPartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // get partitionNums
  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto partitionNums = normal::connector::defaultMiniCatalogue->partitionNums();
  for (auto const &partitionNumEntry: *partitionNums) {
    auto tableName = partitionNumEntry.first;
    auto partitionNum = partitionNumEntry.second;
    auto objects = std::make_shared<std::vector<std::string>>();
    if (partitionNum == 1) {
      if (dir_prefix.find("csv") != std::string::npos) {
        objects->emplace_back(dir_prefix + tableName + ".tbl");
      } else if (dir_prefix.find("parquet") != std::string::npos) {
        objects->emplace_back(dir_prefix + tableName + ".parquet");
      } else {
        // something went wrong, this will cause an error later
        SPDLOG_ERROR("Unknown file name to use for directory with prefix: {}", dir_prefix);
      }
      s3ObjectsMap->emplace(tableName, objects);
    } else {
      for (int i = 0; i < partitionNum; i++) {
        if (dir_prefix.find("csv") != std::string::npos) {
          objects->emplace_back(fmt::format("{0}{1}_sharded/{1}.tbl.{2}", dir_prefix, tableName, i));
        } else if (dir_prefix.find("parquet") != std::string::npos) {
          objects->emplace_back(fmt::format("{0}{1}_sharded/{1}.parquet.{2}", dir_prefix, tableName, i));
        } else {
          // something went wrong, this will cause an error later
          SPDLOG_ERROR("Unknown file name to use for directory with prefix: {}", dir_prefix);
        }
      }
      s3ObjectsMap->emplace(tableName, objects);
    }
  }

  // look up tables
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto objects = s3ObjectPair.second;
    s3Objects->insert(s3Objects->end(), objects->begin(), objects->end());
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, normal::plan::DefaultS3Client);

  // configure s3Connector
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto tableName = s3ObjectPair.first;
    auto objects = s3ObjectPair.second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    for (auto const &s3Object: *objects) {
      auto numBytes = objectNumBytes_Map.find(s3Object)->second;
      partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
    }
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i->put(cat);
}
