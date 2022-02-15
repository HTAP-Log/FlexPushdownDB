//
// Created by ZhangOscar on 12/3/21.
//

#include "normal/connector/MiniCatalogue.h"
#include "conversion/Conversion.h"
#include "normal/tuple/Scalar.h"
#include "arrow/scalar.h"

int main() {
    // starting the database connector for schema lookup
    normal::connector::defaultMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue("pushdowndb-htap", "super-small-ssb-htap/csv/stables/");

    auto sampleSchema = normal::connector::defaultMiniCatalogue->getDeltaSchema("lineorder");

    // initialize example delta rows
    std::shared_ptr<::arrow::Scalar> orderKey = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(1));
    std::shared_ptr<::arrow::Scalar> lineNumber = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(1));
    std::shared_ptr<::arrow::Scalar> custKey = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(209));
    std::shared_ptr<::arrow::Scalar> partKey = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(1552));
    std::shared_ptr<::arrow::Scalar> suppKey = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(9));
    std::shared_ptr<::arrow::Scalar> orderDate = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(19940925));
    std::shared_ptr<::arrow::Scalar> orderPriority = std::make_shared<::arrow::Scalar>(::arrow::StringScalar("1-URGENT"));
    std::shared_ptr<::arrow::Scalar> shipPriority = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(0));
    std::shared_ptr<::arrow::Scalar> quantity = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(16));
    std::shared_ptr<::arrow::Scalar> extendedPrice = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(2471035));
    std::shared_ptr<::arrow::Scalar> orderTotalPrice = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(11507269));
    std::shared_ptr<::arrow::Scalar> discount = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(4));
    std::shared_ptr<::arrow::Scalar> revenue = std::make_shared<::arrow::Scalar>(::arrow::Int64Scalar(2372193));
    std::shared_ptr<::arrow::Scalar> supplyCost = std::make_shared<::arrow::Scalar>(::arrow::Int64Scalar(87214));
    std::shared_ptr<::arrow::Scalar> tax = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(2));
    std::shared_ptr<::arrow::Scalar> commitDate = std::make_shared<::arrow::Scalar>(::arrow::Int64Scalar(19941105));
    std::shared_ptr<::arrow::Scalar> shipMode = std::make_shared<::arrow::Scalar>(::arrow::StringScalar("TRUCK"));
    std::shared_ptr<::arrow::Scalar> type = std::make_shared<::arrow::Scalar>(::arrow::StringScalar("UPDATE"));
    std::shared_ptr<::arrow::Scalar> timestamp = std::make_shared<::arrow::Scalar>(::arrow::Int32Scalar(20211203));

    std::vector<::std::shared_ptr<::arrow::Scalar>> rawRow = {
            orderKey, lineNumber, custKey, partKey, suppKey, orderDate, orderPriority, shipPriority,
            quantity, extendedPrice, orderTotalPrice, discount, revenue, supplyCost, tax, commitDate,
            shipMode, shipMode, type, timestamp
    };

    std::vector<std::shared_ptr<normal::tuple::Scalar>> sampleRow(rawRow.size());
    for (int c = 0; c < rawRow.size(); c ++) {
        sampleRow[c] = normal::tuple::Scalar::make(rawRow[c]);
    }
    std::vector<std::vector<std::shared_ptr<normal::tuple::Scalar>>> sampleDelta(100, sampleRow);

    // call the conversion
    auto sampleResult = normal::htap::conversion::rowToColumn(sampleDelta, sampleSchema);

    return 0;
}