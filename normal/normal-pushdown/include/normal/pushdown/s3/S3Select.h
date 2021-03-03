//
// Created by Matt Woicik on 1/19/21.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECT_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECT_H

#include "normal/pushdown/s3/S3SelectScan.h"

namespace normal::pushdown {
class S3Select: public S3SelectScan {
  public:
    S3Select() = default;
    S3Select(const S3Select&) = default;
    S3Select& operator=(const S3Select&) = default;
    S3Select(std::string name,
           std::string s3Bucket,
           std::string s3Object,
           std::string filterSql,
           std::vector<std::string> returnedS3ColumnNames,
           std::vector<std::string> neededColumnNames,
           int64_t startOffset,
           int64_t finishOffset,
           std::string tableName,
           bool scanOnStart,
           bool toCache,
           long queryId,
           std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys);

    static std::shared_ptr<S3Select> make(const std::string& name,
                        const std::string& s3Bucket,
                        const std::string& s3Object,
                        const std::string& filterSql,
                        const std::vector<std::string>& returnedS3ColumnNames,
                        const std::vector<std::string>& neededColumnNames,
                        int64_t startOffset,
                        int64_t finishOffset,
                        const std::string tableName,
                        bool scanOnStart = true,
                        bool toCache = false,
                        long queryId = 0,
                        const std::vector<std::shared_ptr<normal::cache::SegmentKey>>& weightedSegmentKeys = std::vector<std::shared_ptr<normal::cache::SegmentKey>>());

    // Something has to be done after Operator created to avoid serialization
    void makeParser();

    // A series of get functions
    const std::string &getFilterSql() const;
    const std::shared_ptr<S3CSVParser> &getParser() const;

private:
    std::string filterSql_;   // "where ...."
    std::shared_ptr<S3CSVParser> parser_;

    Aws::S3::Model::InputSerialization getInputSerialization();
    [[nodiscard]] tl::expected<void, std::string> s3Select();

    void processScanMessage(const scan::ScanMessage &message) override;

    std::shared_ptr<TupleSet2> readTuples() override;
    int getPredicateNum() override;

// caf inspect
public:
    template <class Inspector>
    friend bool inspect(Inspector& f, S3Select& op) {
      return f.object(op).fields(f.field("filterSql", op.filterSql_),
                                 f.field("s3Bucket", op.s3Bucket_),
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

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECT_H
