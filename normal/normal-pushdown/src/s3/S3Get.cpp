//
// Created by Matt Woicik on 1/19/21.
//

#include "normal/pushdown/s3/S3Get.h"

#include <iostream>
#include <utility>
#include <memory>
#include <cstdlib>                                          // for abort

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/core/client/ClientConfiguration.h>

#include <arrow/csv/options.h>                              // for ReadOptions
#include <arrow/csv/reader.h>                               // for TableReader
#include <arrow/io/buffered.h>                              // for BufferedI...
#include <arrow/io/memory.h>                                // for BufferReader
#include <arrow/type_fwd.h>                                 // for default_m...
#include <aws/core/Region.h>                                // for US_EAST_1
#include <aws/core/auth/AWSAuthSigner.h>                    // for AWSAuthV4...
#include <aws/core/http/Scheme.h>                           // for Scheme
#include <aws/core/utils/logging/LogLevel.h>                // for LogLevel
#include <aws/core/utils/memory/stl/AWSAllocator.h>         // for MakeShared
#include <aws/s3/model/CSVInput.h>                          // for CSVInput
#include <aws/s3/model/CSVOutput.h>                         // for CSVOutput
#include <aws/s3/model/ExpressionType.h>                    // for Expressio...
#include <aws/s3/model/FileHeaderInfo.h>                    // for FileHeade...
#include <aws/s3/model/InputSerialization.h>                // for InputSeri...
#include <aws/s3/model/OutputSerialization.h>               // for OutputSer...
#include <aws/s3/model/RecordsEvent.h>                      // for RecordsEvent
#include <aws/s3/model/SelectObjectContentHandler.h>        // for SelectObj...
#include <aws/s3/model/StatsEvent.h>                        // for StatsEvent
#include <aws/s3/model/GetObjectRequest.h>                  // for GetObj...


#include "normal/core/message/Message.h"                    // for Message
#include "normal/tuple/TupleSet.h"                          // for TupleSet
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <normal/cache/SegmentKey.h>
#include <normal/connector/MiniCatalogue.h>

#include "normal/pushdown/Globals.h"
#include <normal/pushdown/cache/CacheHelper.h>
#include <parquet/arrow/reader.h>
#include <normal/tuple/arrow/ArrowAWSInputStream.h>
//#include <normal/tuple/arrow/ArrowAWSGZIPInputStream.h>
#include <normal/tuple/arrow/ArrowAWSGZIPInputStream2.h>

#ifdef __AVX2__

#include <normal/plan/Globals.h>
#include "normal/tuple/arrow/CSVToArrowSIMDStreamParser.h"

#endif

namespace Aws::Utils::RateLimits { class RateLimiterInterface; }
namespace arrow { class MemoryPool; }

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

using namespace normal::cache;
using namespace normal::core::cache;
using namespace normal::pushdown::cache;

namespace normal::pushdown {

    std::mutex GetConvertLock;
    int activeGetConversions = 0;

    S3Get::S3Get(std::string name,
                 std::string s3Bucket,
                 std::string s3Object,
                 std::vector<std::string> returnedS3ColumnNames,
                 std::vector<std::string> neededColumnNames,
                 int64_t startOffset,
                 int64_t finishOffset,
                 std::shared_ptr<arrow::Schema> schema,
                 std::shared_ptr<Aws::S3::S3Client> s3Client,
                 bool scanOnStart,
                 bool toCache,
                 long queryId,
                 std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys) :
            S3SelectScan(std::move(name), "S3Get", std::move(s3Bucket), std::move(s3Object),
                         std::move(returnedS3ColumnNames), std::move(neededColumnNames),
                         startOffset, finishOffset, std::move(schema),
                         std::move(s3Client), scanOnStart, toCache, queryId, std::move(weightedSegmentKeys)) {
    }

    S3Get::S3Get(std::string name,
                 std::string s3Bucket,
                 std::string s3Object,
                 std::vector<std::string> returnedS3ColumnNames,
                 std::vector<std::string> neededColumnNames,
                 int64_t startOffset,
                 int64_t finishOffset,
                 std::shared_ptr<arrow::Schema> schema,
                 std::string tableName,
                 std::shared_ptr<Aws::S3::S3Client> s3Client,
                 bool scanOnStart,
                 bool toCache,
                 long queryId,
                 std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys) :
            S3SelectScan(std::move(name), "S3Get", std::move(s3Bucket), std::move(s3Object),
                         std::move(returnedS3ColumnNames), std::move(neededColumnNames),
                         startOffset, finishOffset, std::move(schema),
                         std::move(s3Client), scanOnStart, toCache, queryId, std::move(weightedSegmentKeys)), tableName_(tableName) {
    }

    std::shared_ptr<S3Get> S3Get::make(const std::string &name,
                                       const std::string &s3Bucket,
                                       const std::string &s3Object,
                                       const std::vector<std::string> &returnedS3ColumnNames,
                                       const std::vector<std::string> &neededColumnNames,
                                       int64_t startOffset,
                                       int64_t finishOffset,
                                       const std::shared_ptr<arrow::Schema> &schema,
                                       const std::shared_ptr<Aws::S3::S3Client> &s3Client,
                                       bool scanOnStart,
                                       bool toCache,
                                       long queryId,
                                       const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> &weightedSegmentKeys) {
        return std::make_shared<S3Get>(name,
                                       s3Bucket,
                                       s3Object,
                                       returnedS3ColumnNames,
                                       neededColumnNames,
                                       startOffset,
                                       finishOffset,
                                       schema,
                                       s3Client,
                                       scanOnStart,
                                       toCache,
                                       queryId,
                                       weightedSegmentKeys);

    }

    std::shared_ptr<S3Get> S3Get::make_1(const std::string &name,
                                       const std::string &s3Bucket,
                                       const std::string &s3Object,
                                       const std::vector<std::string> &returnedS3ColumnNames,
                                       const std::vector<std::string> &neededColumnNames,
                                       int64_t startOffset,
                                       int64_t finishOffset,
                                       const std::shared_ptr<arrow::Schema> &schema,
                                       const std::string& tableName,
                                       const std::shared_ptr<Aws::S3::S3Client> &s3Client,
                                       bool scanOnStart,
                                       bool toCache,
                                       long queryId,
                                       const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> &weightedSegmentKeys) {
        return std::make_shared<S3Get>(name,
                                       s3Bucket,
                                       s3Object,
                                       returnedS3ColumnNames,
                                       neededColumnNames,
                                       startOffset,
                                       finishOffset,
                                       schema,
                                       tableName,
                                       s3Client,
                                       scanOnStart,
                                       toCache,
                                       queryId,
                                       weightedSegmentKeys);

    }

    bool S3Get::parallelTuplesetCreationSupported() {
        // We only supports combining uncompressed CSV in parallel,
        // though in the future we will support Parquet, this will require
        // different code than CSV though so it hasn't been done yet.
        if (s3Object_.find("gz") != std::string::npos ||
            s3Object_.find("bz2") != std::string::npos ||
            s3Object_.find("csv") == std::string::npos) {
            return false;
        }
        return true;
    }

    std::shared_ptr<TupleSet2> S3Get::readCSVFile(std::shared_ptr<arrow::io::InputStream> &arrowInputStream) {
        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto read_options = arrow::csv::ReadOptions::Defaults();
        read_options.use_threads = false;
        read_options.skip_rows = 1; // Skip the header
        read_options.column_names = returnedS3ColumnNames_;
        auto convert_options = arrow::csv::ConvertOptions::Defaults();
        std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
        for (const auto &columnName: returnedS3ColumnNames_) {
            columnTypes.emplace(columnName, schema_->GetFieldByName(columnName)->type());
        }
        convert_options.column_types = columnTypes;
        convert_options.include_columns = neededColumnNames_;

        // Instantiate TableReader from input stream and options
        auto makeReaderResult = arrow::csv::TableReader::Make(arrow::default_memory_pool(),
                                                              arrowInputStream,
                                                              read_options,
                                                              parse_options,
                                                              convert_options);
        if (!makeReaderResult.ok())
            throw std::runtime_error(fmt::format(
                    "Cannot parse S3 payload  |  Could not create a table reader, error: '{}'",
                    makeReaderResult.status().message()));
        auto tableReader = *makeReaderResult;

        // Parse the payload and create the tupleset
        auto tupleSetV1 = TupleSet::make(tableReader);
        auto tupleSet = TupleSet2::create(tupleSetV1);
        return tupleSet;
    }

    std::shared_ptr<TupleSet2>
    S3Get::readParquetFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile) {
        std::string parquetFileString(std::istreambuf_iterator<char>(retrievedFile), {});
        auto bufferedReader = std::make_shared<arrow::io::BufferReader>(parquetFileString);

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        arrow::MemoryPool *pool = arrow::default_memory_pool();
        arrow::Status st = parquet::arrow::OpenFile(bufferedReader, pool, &arrow_reader);
        arrow_reader->set_use_threads(false);

        if (!st.ok()) {
            SPDLOG_ERROR("Error opening file for {}\nError: {}", name(), st.message());
        }
        std::shared_ptr<arrow::Table> table;
        // only read the needed columns from the file
        std::vector<int> neededColumnIndices;
        for (auto fieldName : neededColumnNames_) {
            neededColumnIndices.emplace_back(schema_->GetFieldIndex(fieldName));
        }
        st = arrow_reader->ReadTable(neededColumnIndices, &table);
        if (!st.ok()) {
            SPDLOG_ERROR("Error reading parquet data for {}\nError: {}", name(), st.message());
        }
        auto tupleSetV1 = TupleSet::make(table);
        auto tupleSet = TupleSet2::create(tupleSetV1);
        return tupleSet;
    }

    // Created for reading log file from S3, take in one argument which is the key of the log object on S3
    std::shared_ptr<TupleSet2> S3Get::s3GetFullRequest_cp(std::string objectKey) {
        GetObjectRequest getObjectRequest;
        getObjectRequest.SetBucket(Aws::String(s3Bucket_));
        getObjectRequest.SetKey(Aws::String(objectKey));

        std::chrono::steady_clock::time_point startTransferTime = std::chrono::steady_clock::now();
        GetObjectOutcome getObjectOutcome;
        int maxRetryAttempts = 100;
        int attempts = 0;
        while (true) {
            attempts++;
            getObjectOutcome = s3Client_->GetObject(getObjectRequest);
            if (getObjectOutcome.IsSuccess()) {
                break;
            }
            if (attempts > maxRetryAttempts) {
                const auto &err = getObjectOutcome.GetError();
                throw std::runtime_error(
                        fmt::format("{}, {} after {} retries", err.GetMessage(), name(), maxRetryAttempts));
            }
            // Something went wrong with AWS API on our end or remotely, wait and try again
            std::this_thread::sleep_for(std::chrono::milliseconds(minimumSleepRetryTimeMS));
        }
        std::chrono::steady_clock::time_point stopTransferTime = std::chrono::steady_clock::now();
        auto transferTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                stopTransferTime - startTransferTime).count();
        splitReqLock_.lock();
        s3SelectScanStats_.getTransferTimeNS += transferTime;
        s3SelectScanStats_.numRequests++;
        splitReqLock_.unlock();

        std::shared_ptr<TupleSet2> tupleSet;
        auto getResult = getObjectOutcome.GetResultWithOwnership();
        int64_t resultSize = getResult.GetContentLength();
        splitReqLock_.lock();
        s3SelectScanStats_.processedBytes += resultSize;
        s3SelectScanStats_.returnedBytes += resultSize;
        splitReqLock_.unlock();
        std::vector<std::shared_ptr<arrow::Field>> fields;

        // modify this
        for (auto column : neededColumnNames_) {
            fields.emplace_back(::arrow::field(column, schema_->GetFieldByName(column)->type()));
        }

        auto outputSchema = std::make_shared<::arrow::Schema>(fields);
        while (true) {
            if (GetConvertLock.try_lock()) {
                if (activeGetConversions < maxConcurrentArrowConversions) {
                    activeGetConversions++;
                    GetConvertLock.unlock();
                    break;
                } else {
                    GetConvertLock.unlock();
                }
            }
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % variableSleepRetryTimeMS + minimumSleepRetryTimeMS));
        }
        std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
        Aws::IOStream &retrievedFile = getResult.GetBody();
        if (s3Object_.find("csv") != std::string::npos) {
            std::shared_ptr<arrow::io::InputStream> inputStream;
            if (s3Object_.find("gz") != std::string::npos) {
#ifdef __AVX2__
                auto parser = CSVToArrowSIMDStreamParser(name(), DefaultS3ConversionBufferSize, retrievedFile, true,
                                                         schema_, schema_, true,
                                                         normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
                tupleSet = parser.constructTupleSet();
#else
                inputStream = std::make_shared<ArrowAWSGZIPInputStream2>(retrievedFile, resultSize);
      tupleSet = readCSVFile(inputStream);
#endif
            } else {
#ifdef __AVX2__
                auto parser = CSVToArrowSIMDStreamParser(name(), DefaultS3ConversionBufferSize, retrievedFile, true,
                                                         schema_, schema_, false,
                                                         normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
                tupleSet = parser.constructTupleSet();
#else
                inputStream = std::make_shared<ArrowAWSInputStream>(retrievedFile);
      tupleSet = readCSVFile(inputStream);
#endif
            }
        } else { // (s3Object_.find("parquet") != std::string::npos)
            tupleSet = readParquetFile(retrievedFile);
        }
        GetConvertLock.lock();
        activeGetConversions--;
        GetConvertLock.unlock();
        std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
        auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                stopConversionTime - startConversionTime).count();
        splitReqLock_.lock();
        s3SelectScanStats_.getConvertTimeNS += conversionTime;
        splitReqLock_.unlock();
        return tupleSet;
    }

    std::shared_ptr<TupleSet2> S3Get::s3GetFullRequest() {
        GetObjectRequest getObjectRequest;
        getObjectRequest.SetBucket(Aws::String(s3Bucket_));
        getObjectRequest.SetKey(Aws::String(s3Object_));

        std::chrono::steady_clock::time_point startTransferTime = std::chrono::steady_clock::now();
        GetObjectOutcome getObjectOutcome;
        int maxRetryAttempts = 100;
        int attempts = 0;
        while (true) {
            attempts++;
            getObjectOutcome = s3Client_->GetObject(getObjectRequest);
            if (getObjectOutcome.IsSuccess()) {
                break;
            }
            if (attempts > maxRetryAttempts) {
                const auto &err = getObjectOutcome.GetError();
                throw std::runtime_error(
                        fmt::format("{}, {} after {} retries", err.GetMessage(), name(), maxRetryAttempts));
            }
            // Something went wrong with AWS API on our end or remotely, wait and try again
            std::this_thread::sleep_for(std::chrono::milliseconds(minimumSleepRetryTimeMS));
        }
        std::chrono::steady_clock::time_point stopTransferTime = std::chrono::steady_clock::now();
        auto transferTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                stopTransferTime - startTransferTime).count();
        splitReqLock_.lock();
        s3SelectScanStats_.getTransferTimeNS += transferTime;
        s3SelectScanStats_.numRequests++;
        splitReqLock_.unlock();

        std::shared_ptr<TupleSet2> tupleSet;
        auto getResult = getObjectOutcome.GetResultWithOwnership();
        int64_t resultSize = getResult.GetContentLength();
        splitReqLock_.lock();
        s3SelectScanStats_.processedBytes += resultSize;
        s3SelectScanStats_.returnedBytes += resultSize;
        splitReqLock_.unlock();
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (auto column : neededColumnNames_) {
            fields.emplace_back(::arrow::field(column, schema_->GetFieldByName(column)->type()));
        }
        auto outputSchema = std::make_shared<::arrow::Schema>(fields);
        while (true) {
            if (GetConvertLock.try_lock()) {
                if (activeGetConversions < maxConcurrentArrowConversions) {
                    activeGetConversions++;
                    GetConvertLock.unlock();
                    break;
                } else {
                    GetConvertLock.unlock();
                }
            }
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % variableSleepRetryTimeMS + minimumSleepRetryTimeMS));
        }
        std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
        Aws::IOStream &retrievedFile = getResult.GetBody();
        if (s3Object_.find("csv") != std::string::npos) {
            std::shared_ptr<arrow::io::InputStream> inputStream;
            if (s3Object_.find("gz") != std::string::npos) {
#ifdef __AVX2__
                auto parser = CSVToArrowSIMDStreamParser(name(), DefaultS3ConversionBufferSize, retrievedFile, true,
                                                         schema_, outputSchema, true,
                                                         normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
                tupleSet = parser.constructTupleSet();
#else
                inputStream = std::make_shared<ArrowAWSGZIPInputStream2>(retrievedFile, resultSize);
                tupleSet = readCSVFile(inputStream);
#endif
            } else {
#ifdef __AVX2__
                auto parser = CSVToArrowSIMDStreamParser(name(), DefaultS3ConversionBufferSize, retrievedFile, true,
                                                         schema_, outputSchema, false,
                                                         normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
                tupleSet = parser.constructTupleSet();
#else
                inputStream = std::make_shared<ArrowAWSInputStream>(retrievedFile);
                tupleSet = readCSVFile(inputStream);
#endif
            }
        } else { // (s3Object_.find("parquet") != std::string::npos)
            tupleSet = readParquetFile(retrievedFile);
        }
        GetConvertLock.lock();
        activeGetConversions--;
        GetConvertLock.unlock();
        std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
        auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                stopConversionTime - startConversionTime).count();
        splitReqLock_.lock();
        s3SelectScanStats_.getConvertTimeNS += conversionTime;
        splitReqLock_.unlock();
        return tupleSet;
    }

    GetObjectResult S3Get::s3GetRequestOnly(int64_t startOffset, int64_t endOffset) {
        GetObjectRequest getObjectRequest;
        getObjectRequest.SetBucket(Aws::String(s3Bucket_));
        getObjectRequest.SetKey(Aws::String(s3Object_));
        if (normal::plan::s3ClientType != normal::plan::Airmettle) {
            std::stringstream ss;
            ss << "bytes=" << startOffset << '-' << endOffset;
            getObjectRequest.SetRange(ss.str().c_str());
        }

        std::chrono::steady_clock::time_point startTransferTime = std::chrono::steady_clock::now();
        GetObjectOutcome getObjectOutcome;
        int maxRetryAttempts = 100;
        int attempts = 0;
        while (true) {
            attempts++;
            getObjectOutcome = s3Client_->GetObject(getObjectRequest);
            if (getObjectOutcome.IsSuccess()) {
                break;
            }
            if (attempts > maxRetryAttempts) {
                const auto &err = getObjectOutcome.GetError();
                throw std::runtime_error(
                        fmt::format("{}, {} after {} retries", err.GetMessage(), name(), maxRetryAttempts));
            }
            // Something went wrong with AWS API on our end or remotely, wait and try again
            std::this_thread::sleep_for(std::chrono::milliseconds(minimumSleepRetryTimeMS));
        }
        std::chrono::steady_clock::time_point stopTransferTime = std::chrono::steady_clock::now();
        auto transferTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                stopTransferTime - startTransferTime).count();

        std::shared_ptr<TupleSet2> tupleSet;
        auto getResult = getObjectOutcome.GetResultWithOwnership();
        int64_t resultSize = getResult.GetContentLength();
        splitReqLock_.lock();
        s3SelectScanStats_.getTransferTimeNS += transferTime;
        s3SelectScanStats_.numRequests++;
        s3SelectScanStats_.processedBytes += resultSize;
        s3SelectScanStats_.returnedBytes += resultSize;
        splitReqLock_.unlock();
        return getResult;
    }

#ifdef __AVX2__

    void S3Get::s3GetIndividualReq(int reqNum, int64_t startOffset, int64_t endOffset) {
        GetObjectResult getObjectResult = s3GetRequestOnly(startOffset, endOffset);
        if (parallelTuplesetCreationSupported()) {
            // Generate parser, and process initial input store partial in a separate vector that we will
            // pass in at the end once all threads are joined.
            std::vector<char> prevPartialResult;
            Aws::IOStream &retrievedFile = getObjectResult.GetBody();
            char currentChar = retrievedFile.get();
            while (currentChar != '\n' && !retrievedFile.eof()) {
                prevPartialResult.emplace_back(currentChar);
                currentChar = retrievedFile.get();
            }
            if (reqNum != 1) {
                int reqNumNeedingPartialResult = reqNum - 1;
                splitReqLock_.lock();
                reqNumToAdditionalOutput_.insert(
                        std::pair<int, std::vector<char>>(reqNumNeedingPartialResult, prevPartialResult));
                splitReqLock_.unlock();
            }
            // FIXME: Now process the rest of the file, for now we are only worrying about SIMD CSV
            //        we will add in more robust support later
            while (true) {
                if (GetConvertLock.try_lock()) {
                    if (activeGetConversions < maxConcurrentArrowConversions) {
                        activeGetConversions++;
                        GetConvertLock.unlock();
                        break;
                    } else {
                        GetConvertLock.unlock();
                    }
                }
                std::this_thread::sleep_for(
                        std::chrono::milliseconds(rand() % variableSleepRetryTimeMS + minimumSleepRetryTimeMS));
            }
            std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
            std::vector<std::shared_ptr<arrow::Field>> fields;
            for (auto column : neededColumnNames_) {
                fields.emplace_back(::arrow::field(column, schema_->GetFieldByName(column)->type()));
            }
            auto outputSchema = std::make_shared<::arrow::Schema>(fields);
            std::shared_ptr<CSVToArrowSIMDChunkParser> parser = std::make_shared<CSVToArrowSIMDChunkParser>(name(),
                                                                                                            DefaultS3ConversionBufferSize,
                                                                                                            schema_,
                                                                                                            outputSchema,
                                                                                                            normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
            parser->parseChunk(std::make_shared<ArrowAWSInputStream>(retrievedFile));
            std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
            GetConvertLock.lock();
            activeGetConversions--;
            GetConvertLock.unlock();
            auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    stopConversionTime - startConversionTime).count();
            splitReqLock_.lock();
            reqNumToParser_.insert(std::pair<int, std::shared_ptr<CSVToArrowSIMDChunkParser>>(reqNum, parser));
            s3SelectScanStats_.getConvertTimeNS += conversionTime;
            splitReqLock_.unlock();
        } else {
            throw std::runtime_error(
                    fmt::format("Splitting up requests that need a concatenated output not supported yet."));
        }
    }

    std::shared_ptr<TupleSet2> S3Get::s3GetParallelReqs() {
        int totalReqs = 0;
        uint64_t currentStartOffset = 0;

        // Spawn all of the requests
        std::vector<std::thread> threadVector = std::vector<std::thread>();
        while (currentStartOffset < finishOffset_) {
            totalReqs += 1;
            uint64_t reqEndOffset = currentStartOffset + DefaultS3RangeSize;
            if (reqEndOffset > finishOffset_) {
                reqEndOffset = finishOffset_;
            }
            // If there is only a little bit of data left to scan just combine it in this request rather than
            // starting a new request.
            if ((double) (finishOffset_ - reqEndOffset) < (double) (DefaultS3RangeSize * 0.30)) {
                reqEndOffset = finishOffset_;
            }
            threadVector.emplace_back(std::thread([&, totalReqs, currentStartOffset, reqEndOffset]() {
                s3GetIndividualReq(totalReqs, currentStartOffset, reqEndOffset);
            }));
            currentStartOffset = reqEndOffset + 1;
        }

        // Wait for all requests to finish
        for (auto &t: threadVector) {
            t.join();
        }

        if (parallelTuplesetCreationSupported()) {
            // Now complete all of the parsers and combine together all of their arrow tables
            std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
            std::vector<std::shared_ptr<arrow::Table>> tables;
            for (int i = 1; i <= totalReqs; i++) {
                std::shared_ptr<CSVToArrowSIMDChunkParser> parser = reqNumToParser_[i];
                // add partial line from previous request number to parser here
                if (i < totalReqs) {
                    std::vector<char> remainingData = reqNumToAdditionalOutput_[i];
                    if (remainingData.size() > 0) {
                        parser->parseChunk(remainingData.data(), remainingData.size());
                    }
                }
                std::shared_ptr<TupleSet2> currentTupleSet = parser->outputCompletedTupleSet();
                std::shared_ptr<arrow::Table> currentTable = currentTupleSet->getArrowTable().value();
                // Don't need to concatenate empty tables
                if (currentTable->num_rows() > 0) {
                    tables.push_back(currentTable);
                }
            }

            std::shared_ptr<TupleSet2> readTupleSet;
            if (tables.size() == 0) {
                readTupleSet = TupleSet2::make2();
            } else if (tables.size() == 1) {
                auto tupleSetV1 = normal::tuple::TupleSet::make(tables[0]);
                readTupleSet = normal::tuple::TupleSet2::create(tupleSetV1);
            } else {
                const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables);
                if (!res.ok())
                    abort();
                auto tupleSetV1 = normal::tuple::TupleSet::make(*res);
                readTupleSet = normal::tuple::TupleSet2::create(tupleSetV1);
            }
            std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
            auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    stopConversionTime - startConversionTime).count();
            splitReqLock_.lock();
            s3SelectScanStats_.getConvertTimeNS += conversionTime;
            splitReqLock_.unlock();
            return readTupleSet;
        } else {
            throw std::runtime_error(
                    fmt::format("Splitting up requests that need a concatenated output not supported yet."));
        }
    }

#endif

    std::shared_ptr<TupleSet2> S3Get::readTuples() {
        std::shared_ptr<TupleSet2> readTupleSet;
        std::shared_ptr<TupleSet2> logTupleSet;

        std::string logObjectKey = "ssb-sf10-sortlineorder/csv/log/log.csv";  // one table one log
        std::string readObjectKey = "ssb-sf10-sortlineorder/csv/lineorder_sharded/lineorder.tbl.1";

        if (neededColumnNames_.empty()) {
            readTupleSet = TupleSet2::make2();
        } else {

            SPDLOG_DEBUG("Reading From S3: {}", name());

            // Read columns from s3
#ifdef __AVX2__
//            if (normal::plan::s3ClientType == normal::plan::S3 && parallelTuplesetCreationSupported()
//                && (finishOffset_ - startOffset_ > DefaultS3RangeSize)) {
//                readTupleSet = s3GetParallelReqs();
//            } else {
//                readTupleSet = s3GetFullRequest();
//            }

            if (tableName_ == "lineorder") {
                logTupleSet = s3GetFullRequest_cp(logObjectKey);
            }
            readTupleSet = s3GetFullRequest();

#else
            readTupleSet = s3GetFullRequest();
            logTupleSer = s3GetFullRequest_cp(logObjectKey);
#endif

            // Store the read columns in the cache, if not in full-pushdown mode
//            if (toCache_) {
//                // TODO: only send caching columnsœ
//                requestStoreSegmentsInCache(readTupleSet);
//            } else {
//                // send segment filter weight
//                if (weightedSegmentKeys_ && s3SelectScanStats_.processedBytes > 0)
//                    sendSegmentWeight();
//                }
//            }
        }
        SPDLOG_DEBUG("Finished Reading: {}", name());

        // RUN THE MERGE HERE

        // STEP 1. Construct the DeleteMap
//        std::unordered_set<std::string> deleteMap;

        if (tableName_ == "lineorder") {
            SPDLOG_CRITICAL(logTupleSet->getColumnByIndex(0).value()->showString());
        }

//        SPDLOG_CRITICAL(logTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

//        mergeLog(logTupleSet, readTupleSet);


        return readTupleSet;
    }

    void S3Get::mergeLog(std::shared_ptr<TupleSet2> logTuple, std::shared_ptr<TupleSet2> fileTuple) {
        // GET DELETE MAP
//        std::unordered_set<std::string> deleteMap = generateDeleteMap(logTuple);
//
//        for (int i = 0; i < logTuple->numRows(); i++) {
//            if ( deleteMap.find(logTuple.get()->getColumnByIndex(0))) {
//                // Mark as Deleted.
//                // ??
//            }
//        }
//
//        logTuple->concatenate(logTuple);
    }

    std::unordered_set<std::string> S3Get::generateDeleteMap(std::shared_ptr<TupleSet2> logTuple) {
        std::unordered_set<std::string> deleteMap;

//        std::shared_ptr<normal::tuple::Column> pKey = logTuple->getColumnByIndex(0).value();
//
//        for (int i = 0; i < pKey->numRows(); i++) {
//            deleteMap.insert(pKey->element(i));
//        }

        return deleteMap;
    }



    void S3Get::processScanMessage(const scan::ScanMessage &message) {
        // This is for hybrid caching as we later determine which columns to pull up
        // Though currently this is only called for SELECT in our system
        neededColumnNames_ = message.getColumnNames();
    };

    int S3Get::getPredicateNum() {
        return 0;
    }

}
