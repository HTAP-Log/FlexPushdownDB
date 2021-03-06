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

using namespace normal::plan::operator_::mode;
using namespace normal::sql;
using namespace normal::core;

namespace normal::frontend {

class Client {
  struct config : actor_system_config {
    config(int port, std::vector<std::string> hosts, bool serverMode) :
    port_(port), hosts_(hosts), serverMode_(serverMode){
      load<io::middleman>();
      add_actor_type("display", display_fun);
      add_actor_type<OperatorActor, OperatorPtr&>("OperatorActor");
    }
    uint16_t port_;
    std::vector<std::string> hosts_;
    bool serverMode_;
  };

  static void client_repl(function_view<display> f, std::string content) {
    auto subMsg = std::make_shared<msg2>(content, 111);
    msg newContent{content, false, subMsg};
    std::cout << f(newContent) << std::endl;
  }

public:
  void remoteDisplay(std::string content) {
    auto type = "display";             // type of the actor we wish to spawn
    auto args = make_message();           // arguments to construct the actor
    auto tout = std::chrono::seconds(30); // wait no longer than 30s
    auto worker = clientActorSystem_->middleman().remote_spawn<display>(nodes_[0], type, args,
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
  std::shared_ptr<config> clientCfg_;
  std::shared_ptr<config> serverCfg_;
  std::shared_ptr<caf::actor_system> clientActorSystem_;
  std::shared_ptr<caf::actor_system> serverActorSystem_;
  std::vector<node_id> nodes_;      // other nodes
  bool distributed_;

  void configureS3ConnectorSinglePartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix);
  void configureS3ConnectorMultiPartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, std::string dir_prefix);
  std::shared_ptr<TupleSet> execute();
  void server();
};

std::vector<std::string> readAllRemoteIps();

}

#endif //NORMAL_FRONTEND_CLIENT_H
