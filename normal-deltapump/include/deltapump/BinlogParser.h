#ifndef NORMAL_BINLOGPARSER_H
#define NORMAL_BINLOGPARSER_H

#include <jni.h>
#include <cassert>
#include <iostream>
#include <vector>
#include <string>
#include <cstdio>
#include <ctime>
#include <cmath> //floor
#include <unordered_map> // std::unordered_map
#include <utility> // std::pair
#include <tuple> // std::tuple
#include <iterator>
#include <set> //std::set
#include <stdexcept> // std::runtime_error
#include <sstream> // std::stringstream

#include <fstream>
#include <complex>

#include "lineorder_d.hh"
#include "customer_d.hh"
#include "supplier_d.hh"
#include "part_d.hh"
#include "date_d.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/DataFile.hh"

#include "avro/Stream.hh"
#include "avro/Specific.hh"
#include "avro/Generic.hh"

#include "makeTuple.h"

using namespace normal::avro_tuple::make;


class BinlogParser
{
    JNIEnv *g_env;
    JavaVM *jvm;

public:

    /*
    * function to call functions in java and receive serialized avro data returned from java side
    * return pointers of partitioned tables
    */
    void parse(const char *filePath,  const char *rangeFilePath, std::unordered_map<int, std::set<struct lineorder_record>> **lineorder_record_ptr );
    BinlogParser();  //constructor


};


struct lineorder_record{
    int orderkey;
    int linenumber;
    LineorderDelta_t lineorder_delta;

    bool operator<(const lineorder_record& l) const
    {
        return ((this->orderkey < l.orderkey) || (this->orderkey == l.orderkey && this->linenumber < l.linenumber));
    }
};

struct customer_record{
    int c_custkey;
    CustomerDelta_t customer_delta;
    bool operator<(const customer_record& c) const
    {
        return (this->c_custkey < c.c_custkey);
    }
};

struct date_record{
    int d_datekey;
    DateDelta_t date_delta;
    bool operator<(const date_record& d) const
    {
        return (this->d_datekey < d.d_datekey);
    }
};

struct part_record{
    int p_partkey;
    PartDelta_t part_delta;
    bool operator<(const part_record& p) const
    {
        return (this->p_partkey < p.p_partkey);
    }
};

struct supplier_record{
    int s_suppkey;
    SupplierDelta_t supplier_delta;
    bool operator<(const supplier_record& s) const
    {
        return (this->s_suppkey < s.s_suppkey);
    }
};

/*
 * load avro schema from disk
 */
avro::ValidSchema loadSchema(const char* filename);


#endif //NORMAL_BINLOGPARSER_H
