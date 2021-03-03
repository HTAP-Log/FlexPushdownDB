//
// Created by Yifei Yang on 2/17/21.
//

#ifndef NORMAL_FRONTEND_SERIALIZATION_H
#define NORMAL_FRONTEND_SERIALIZATION_H

#include <normal/frontend/Global.h>

using namespace caf;

namespace normal::frontend {

struct msg2 {
    msg2(std::string& v, int id) : value2(v), id2(id){}
    msg2() = default;
    msg2(const msg2& other) = default;
    std::string value2;
    int id2;
};
struct msg {
    std::string value;
    bool valid;
    std::shared_ptr<msg2> subMsg;
};

using display = caf::typed_actor<
        caf::replies_to<msg>::with<std::string>>;

static display::behavior_type display_fun(display::pointer self) {
  return {
          [=](const msg content) {
              std::string res = content.value + (content.valid ? "true" : "false");
              aout(self) << "received task from a remote node: " << res << std::endl;
              return res + " received";
          }
  };
}

template <class Inspector>
bool inspect(Inspector& f, msg& m) {
  return f.object(m).fields(f.field("value", m.value),
                            f.field("valid", m.valid),
                            f.field("subMsg", m.subMsg));
}

template <class Inspector>
bool inspect(Inspector& f, msg2& m) {
  return f.object(m).fields(f.field("value", m.value2),
                            f.field("id2", m.id2));
}

}

using namespace normal::frontend;

CAF_BEGIN_TYPE_ID_BLOCK(Client, ::caf::first_custom_type_id + 10000)
CAF_ADD_TYPE_ID(Client, (normal::frontend::msg))
CAF_ADD_TYPE_ID(Client, (normal::frontend::msg2))
CAF_ADD_TYPE_ID(Client, (normal::frontend::display))
CAF_END_TYPE_ID_BLOCK(Client)

template <>
struct inspector_access<std::shared_ptr<msg2>> : variant_inspector_access<std::shared_ptr<msg2>> {
    // nop
};

#endif //NORMAL_FRONTEND_SERIALIZATION_H
