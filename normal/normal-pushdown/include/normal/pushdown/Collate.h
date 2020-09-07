//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_S3_SRC_COLLATE_H
#define NORMAL_NORMAL_S3_SRC_COLLATE_H

#include <normal/core/Collect.h>

namespace normal::pushdown {

class Collate : public normal::core::Collect {

public:
  Collate(const std::string& name, long queryId);

};

}

#endif //NORMAL_NORMAL_S3_SRC_COLLATE_H
