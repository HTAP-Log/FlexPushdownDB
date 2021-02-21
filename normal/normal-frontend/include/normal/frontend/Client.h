//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_FRONTEND_CLIENT_H
#define NORMAL_FRONTEND_CLIENT_H

#include <normal/frontend/Global.h>
#include <normal/frontend/Serialization.h>
#include <normal/cache/CachingPolicy.h>
#include <normal/plan/mode/Modes.h>
#include <normal/sql/Interpreter.h>
#include <normal/core/OperatorActor.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/pushdown/s3/S3Select.h>

using namespace normal::cache;
using namespace normal::plan::operator_::mode;
using namespace normal::sql;
using namespace normal::core;
using namespace normal::pushdown;

namespace normal::frontend {

class Client {

  struct config : actor_system_config {
    config(int port, std::string host, bool serverMode) :
    port_(port), host_(host), serverMode_(serverMode){
      load<io::middleman>();
      add_actor_type("display", display_fun);
//      add_message_type<msg>("msg");
//      add_actor_type<OperatorActor, S3Select&>("S3Select");
//      add_message_type<S3Select>("S3SelectOperatorMessage");
//      add_message_type<Collate>("CollateOperatorMessage");
      opt_group{custom_options_, "global"}
              .add(port, "port,p", "set port")
              .add(host, "host,H", "set node (ignored in server mode)")
              .add(serverMode_, "server-mode,s", "enable server mode");
    }
    uint16_t port_;
    std::string host_;
    bool serverMode_;
  };

  static void client_repl(function_view<display> f, std::string content) {
//    msg newContent{content, false, std::make_shared<msg2>(content, 998)};
    msg newContent{content, false, msg2(content, 998)};
    std::cout << f(newContent) << std::endl;
  }

public:
  void remoteDisplay(std::string content) {
    auto type = "display";             // type of the actor we wish to spawn
    auto args = make_message();           // arguments to construct the actor
    auto tout = std::chrono::seconds(30); // wait no longer than 30s
    auto worker = clientActorSystem_->middleman().remote_spawn<display>(node_.value(), type, args,
                                                                        tout);
    if (!worker) {
      std::cerr << "*** remote spawn failed: " << to_string(worker.error()) << std::endl;
      return;
    }
    // start using worker in main loop
    client_repl(make_function_view(*worker), content);
    // be a good citizen and terminate remotely spawned actor before exiting
    anon_send_exit(*worker, exit_reason::kill);
  }


















public:
  explicit Client();
  std::string boot();
  std::string stop();
  std::string reboot();
  std::string setCachingPolicy(int cachingPolicyType);
  std::string setMode(int modeType);
  std::string executeSql(const std::string &sql);
  std::string executeSqlFile(const std::string &filePath);
  void connect();
  void setDistributed(bool distributed);

private:
  /* Common parameters */
  size_t cacheSize_;
  std::string bucketName_;
  std::string dirPrefix_;
  std::shared_ptr<CachingPolicy> cachingPolicy_;
  std::shared_ptr<Mode> mode_;
  std::shared_ptr<Interpreter> interpreter_;

  /* Remote parameters */
  struct config clientCfg_{DefaultServerPort_, "localhost", false};
  struct config serverCfg_{DefaultServerPort_, "localhost", true};
  std::shared_ptr<caf::actor_system> clientActorSystem_;
  std::shared_ptr<caf::actor_system> serverActorSystem_;
  std::optional<node_id> node_;
  bool distributed_;

  void configureS3ConnectorSinglePartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix);
  void configureS3ConnectorMultiPartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix);
  std::shared_ptr<TupleSet> execute();
  void server();
};

}

#endif //NORMAL_FRONTEND_CLIENT_H
