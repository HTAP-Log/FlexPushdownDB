//
// Created by matt on 23/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORCONNECTION_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORCONNECTION_H

#include <string>

#include <caf/all.hpp>

#include <normal/core/OperatorRelationshipType.h>

namespace normal::core {

class OperatorConnection {
public:
  OperatorConnection(std::string Name, caf::actor ActorHandle, OperatorRelationshipType ConnectionType);
  OperatorConnection() = default;
  OperatorConnection(const OperatorConnection&) = default;
  OperatorConnection& operator=(const OperatorConnection&) = default;

  [[nodiscard]] const std::string &getName() const;
  [[nodiscard]] const caf::actor &getActorHandle() const;
  [[nodiscard]] OperatorRelationshipType getConnectionType() const;

private:
  std::string name_;
  caf::actor actorHandle_;
  OperatorRelationshipType connectionType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, OperatorConnection& con) {
    return f.object(con).fields(f.field("name", con.name_),
                                f.field("actorHandle", con.actorHandle_),
                                f.field("connectionType", con.connectionType_));
  }
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORCONNECTION_H
