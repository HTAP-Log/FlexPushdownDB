//
// Created by matt on 14/12/19.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTPARSER_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTPARSER_H

#include <string>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::tuple;

namespace normal::pushdown {

class S3SelectParser {

private:

  static const int CSV_READER_BUFFER_SIZE = 128 * 1024;

  std::vector<unsigned char> partial{};

  static std::shared_ptr<TupleSet> parseCompletePayload(
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &from,
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &to);

public:

  [[deprecated("Use parse(Aws::Vector<unsigned char> &Vector)")]] std::shared_ptr<TupleSet> parsePayload(Aws::Vector<unsigned char> &Vector);
  tl::expected<std::optional<std::shared_ptr<TupleSet>>, std::string> parse(Aws::Vector<unsigned char> &Vector);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTPARSER_H
