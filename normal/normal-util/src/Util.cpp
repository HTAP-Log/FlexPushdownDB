//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/util/Util.h>
#include <fstream>
#include <sstream>
#include <stdexcept>

using namespace normal::util;

std::string normal::util::readFile(std::string filePath) {
  std::ifstream ifile(filePath);
  std::ostringstream buf;
  char ch;
  while (buf && ifile.get(ch)) {
    buf.put(ch);
  }
  return buf.str();
}

bool normal::util::isInteger(std::string str) {
  try {
    int parsedInt = std::stoi(str);
  } catch (const std::logic_error& err) {
    return false;
  }
  return true;
}
