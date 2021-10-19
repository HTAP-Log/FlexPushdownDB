#include "makeTuple.h"

using namespace normal::avro_tuple::make;

LineorderDelta_t MakeTuple::makeLineorderDeltaTuple(i::lineorder& linorderDeltaStruct) {
    return std::make_tuple(
            linorderDeltaStruct.lo_orderkey,
            linorderDeltaStruct.lo_linenumber,
            linorderDeltaStruct.lo_custkey,
            linorderDeltaStruct.lo_partkey,
            linorderDeltaStruct.lo_suppkey,
            linorderDeltaStruct.lo_orderdate,
            linorderDeltaStruct.lo_orderpriority,
            linorderDeltaStruct.lo_shippriority,
            linorderDeltaStruct.lo_quantity,
            linorderDeltaStruct.lo_extendedprice,
            linorderDeltaStruct.lo_ordtotalprice,
            linorderDeltaStruct.lo_discount,
            linorderDeltaStruct.lo_revenue,
            linorderDeltaStruct.lo_supplycost,
            linorderDeltaStruct.lo_tax,
            linorderDeltaStruct.lo_commitdate,
            linorderDeltaStruct.lo_shipmode,
            linorderDeltaStruct.type,
            linorderDeltaStruct.timestamp);
}
CustomerDelta_t MakeTuple::makeCustomerDeltaTuple(i::customer& customerDeltaStruct) {
    return std::make_tuple(
            customerDeltaStruct.c_custkey,
            customerDeltaStruct.c_name,
            customerDeltaStruct.c_address,
            customerDeltaStruct.c_city,
            customerDeltaStruct.c_nation,
            customerDeltaStruct.c_region,
            customerDeltaStruct.c_phone,
            customerDeltaStruct.c_mktsegment,
            customerDeltaStruct.c_payment,
            customerDeltaStruct.type,
            customerDeltaStruct.timestamp);
}

DateDelta_t MakeTuple::makeDateDeltaTuple(i::date& dateDeltaStruct){
    return std::make_tuple(
            dateDeltaStruct.d_datekey,
            dateDeltaStruct.d_date,
            dateDeltaStruct.d_dayofweek,
            dateDeltaStruct.d_month,
            dateDeltaStruct.d_year,
            dateDeltaStruct.d_yearmonthnum,
            dateDeltaStruct.d_yearmonth,
            dateDeltaStruct.d_daynuminweek,
            dateDeltaStruct.d_daynuminmonth,
            dateDeltaStruct.d_daynuminyear,
            dateDeltaStruct.d_monthnuminyear,
            dateDeltaStruct.d_weeknuminyear,
            dateDeltaStruct.d_sellingseason,
            dateDeltaStruct.d_lastdayinweekfl,
            dateDeltaStruct.d_lastdayinmonthfl,
            dateDeltaStruct.d_holidayfl,
            dateDeltaStruct.d_weekdayfl,
            dateDeltaStruct.type,
            dateDeltaStruct.timestamp);
}

PartDelta_t MakeTuple::makePartDeltaTuple(i::part& PartDeltaStruct){
    return std::make_tuple(
            PartDeltaStruct.p_partkey,
            PartDeltaStruct.p_name,
            PartDeltaStruct.p_mfgr,
            PartDeltaStruct.p_category,
            PartDeltaStruct.p_brand1,
            PartDeltaStruct.p_color,
            PartDeltaStruct.p_type,
            PartDeltaStruct.p_size,
            PartDeltaStruct.p_container,
            PartDeltaStruct.type,
            PartDeltaStruct.timestamp);
}

SupplierDelta_t MakeTuple::makeSupplierDeltaTuple(i::supplier& SupplierDeltaStruct){
    return std::make_tuple(
            SupplierDeltaStruct.s_suppkey,
            SupplierDeltaStruct.s_name,
            SupplierDeltaStruct.s_address,
            SupplierDeltaStruct.s_city,
            SupplierDeltaStruct.s_nation,
            SupplierDeltaStruct.s_region,
            SupplierDeltaStruct.s_phone,
            SupplierDeltaStruct.s_ytd,
            SupplierDeltaStruct.type,
            SupplierDeltaStruct.timestamp);
}
