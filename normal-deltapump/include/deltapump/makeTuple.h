#ifndef NORMAL_MAKETUPLE_H
#define NORMAL_MAKETUPLE_H

#include <tuple>
#include <string>

#include "lineorder_d.hh"

namespace normal::avro_tuple::make {

typedef std::tuple<
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    std::string,
    std::string,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    int64_t,
    int64_t,
    int32_t,
    int32_t,
    std::string,
    std::string,
    int32_t> LineorderDelta_t;

typedef std::tuple<
    int32_t,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    int32_t,
    std::string,
    int32_t> CustomerDelta_t;

typedef std::tuple<
    int32_t,
    std::string,
    std::string,
    std::string,
    int32_t,
    int32_t,
    std::string,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    std::string,
    bool,
    bool,
    bool,
    bool,
    std::string,
    int32_t> DateDelta_t;

typedef std::tuple<
    int32_t,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    int32_t,
    std::string,
    std::string,
    int32_t> PartDelta_t;

typedef std::tuple<
    int32_t,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    int32_t,
    std::string,
    int32_t> SupplierDelta_t;

class MakeTuple {
public:
    static LineorderDelta_t makeLineorderDeltaTuple(i::lineorder& linorderDeltaStruct)

    static CustomerDelta_t makeCustomerDeltaTuple(i::customer& customerDeltaStruct)

    static DateDelta_t makeDateDeltaTuple(i::date& dateDeltaStruct)

    static PartDelta_t makePartDeltaTuple(i::part& PartDeltaStruct)

    static SupplierDelta_t makeSupplierDeltaTuple(i::supplier& SupplierDeltaStruct)

};

}

#endif //NORMAL_MAKETUPLE_H
