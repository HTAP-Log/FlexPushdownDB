//
// Created by Matt Woicik on 1/19/21.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3GET_H
#define NORMAL_NORMAL_CORE_SRC_S3GET_H

#include "normal/pushdown/s3/S3SelectScan.h"

namespace normal::pushdown {
class S3Get : public S3SelectScan {
  public:
    S3Get(std::string name,
             std::string s3Bucket,
             std::string s3Object,
             std::vector<std::string> returnedS3ColumnNames,
             std::vector<std::string> neededColumnNames,
             int64_t startOffset,
             int64_t finishOffset,
             std::string tableName,
             bool scanOnStart,
             bool toCache,
             long queryId,
             std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys);

    static std::shared_ptr<S3Get> make(const std::string& name,
                                       const std::string& s3Bucket,
                                       const std::string& s3Object,
                                       const std::vector<std::string>& returnedS3ColumnNames,
                                       const std::vector<std::string>& neededColumnNames,
                                       int64_t startOffset,
                                       int64_t finishOffset,
                                       const std::string tableName,
                                       bool scanOnStart = true,
                                       bool toCache = false,
                                       long queryId = 0,
                                       const std::vector<std::shared_ptr<normal::cache::SegmentKey>>& weightedSegmentKeys = std::vector<std::shared_ptr<normal::cache::SegmentKey>>());

  private:
    void readCSVFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile);
    [[nodiscard]] tl::expected<void, std::string> s3Get();

    void processScanMessage(const scan::ScanMessage &message) override;

    std::shared_ptr<TupleSet2> readTuples() override;
    int getPredicateNum() override;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3GET_H
