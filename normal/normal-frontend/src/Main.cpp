//
// Created by Yifei Yang on 2/9/21.
//

#include <normal/frontend/Global.h>
#include <normal/frontend/Client.h>

#define CMD_BEGIN(isNewLine) { std::cout << "FlexPushdownDB> "; if(!isNewLine) std::cout << "  ";}
#define RESULT(content) std::cout << "\t" << content << std::endl;

using namespace normal::frontend;

bool execute(const std::string& command, const std::shared_ptr<Client>& client) {
  if (command == "quit" || command == "exit") {
    return true;
  }
  size_t whiteIndex = command.find(' ');
  if (whiteIndex != std::string::npos) {
    auto word1 = command.substr(0, whiteIndex);
    auto word2 = command.substr(whiteIndex + 1);
    if (word1 == "file") {
      RESULT(client->executeSqlFile(word2));
      return false;
    } else if (word1 == "sql") {
      RESULT(word2);
      return false;
    }
  }
  RESULT("Bad command")
  return false;
}

int main() {
  std::string line, appendLine;
  CMD_BEGIN(true)
  std::shared_ptr<Client> client = std::make_shared<Client>();
  client->boot();

  while (std::getline(std::cin, appendLine)) {
    if (line.length() > 0) {
      line.append(" ");
    }
    line += appendLine;
    size_t endIndex = line.find(';');
    if (endIndex != std::string::npos) {
      std::string command = line.substr(0, endIndex);
      if (execute(command, client)) {
        client->stop();
        break;
      }
      line = "";
      CMD_BEGIN(true)
    } else {
      CMD_BEGIN((line.length() == 0))
    }
  }
  return 0;
}