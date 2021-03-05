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
    S3Get() = default;
    S3Get(const S3Get&) = default;
    S3Get& operator=(const S3Get&) = default;

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

  // caf inspect
  public:
    template <class Inspector>
    friend bool inspect(Inspector& f, S3Get& op) {
      return f.object(op).fields(f.field("s3Bucket", op.s3Bucket_),
                                 f.field("s3Object", op.s3Object_),
                                 f.field("returnedS3ColumnNames", op.returnedS3ColumnNames_),
                                 f.field("neededColumnNames", op.neededColumnNames_),
                                 f.field("startOffset", op.startOffset_),
                                 f.field("finishOffset", op.finishOffset_),
                                 f.field("tableName", op.tableName_),
                                 f.field("scanOnStart", op.scanOnStart_),
                                 f.field("toCache", op.toCache_),
                                 f.field("weightedSegmentKeys", op.weightedSegmentKeys_),
                                 f.field("name", op.name()),
                                 f.field("type", op.getType()),
                                 f.field("opContext", op.getOpContext()),
                                 f.field("producers", op.getProducers()),
                                 f.field("consumers", op.getConsumers()));
    }
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3GET_H
