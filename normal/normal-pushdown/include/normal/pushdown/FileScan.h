//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H

#include <string>

#include <normal/core/Operator.h>

namespace normal::pushdown {

class FileScan : public normal::core::Operator {

private:
  std::string filePath_;
  void onStart();
  void onReceive(const normal::core::message::Envelope &message) override;

public:
  FileScan(std::string name, std::string filePath);
  ~FileScan() override;
};

}

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
