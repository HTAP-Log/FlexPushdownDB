#include "BinlogParser.h"

using namespace normal::avro_tuple::make;

/*
 * demo for BinlogParer
 */
int main() {
    // clock to measure duration
    std::clock_t start;
    double duration;
    start = std::clock();

    // vector to stored parsed delta
    std::unordered_map<int, std::set<struct lineorder_record>> *lineorder_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct customer_record>> *customer_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct supplier_record>> *supplier_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct part_record>> *part_record_ptr = nullptr;
    std::unordered_map<int, std::set<struct date_record>> *date_record_ptr = nullptr;

    const char* path = "./bin.000002"; //binlog file path

    BinlogParser binlogParser;
    binlogParser.parse(path, &lineorder_record_ptr, &customer_record_ptr, &supplier_record_ptr, &part_record_ptr, &date_record_ptr);

    for(auto lineorder_pair : (*lineorder_record_ptr)){
        std::set<struct lineorder_record> lineorder_partition = lineorder_pair.second;
        for(auto lineorder_record : lineorder_partition){
            LineorderDelta_t lineorder_delta = lineorder_record.lineorder_delta;
            std::cout << std::get<0>(lineorder_delta) << ", " << std::get<1>(lineorder_delta) << ", " << std::get<2>(lineorder_delta) << ", " <<
                    std::get<3>(lineorder_delta) << ", " << std::get<4>(lineorder_delta) << ", " << std::get<5>(lineorder_delta) << ", " <<
                            std::get<6>(lineorder_delta) << ", " <<std::get<7>(lineorder_delta) << ", " <<std::get<8>(lineorder_delta) << ", " <<
                                    std::get<9>(lineorder_delta) << ", " <<std::get<10>(lineorder_delta) << ", " <<std::get<11>(lineorder_delta) << ", " <<
                                            std::get<12>(lineorder_delta) << ", " <<std::get<13>(lineorder_delta) << ", " << std::get<14>(lineorder_delta) << ", " <<
                                                    std::get<15>(lineorder_delta) << ", "<< std::get<16>(lineorder_delta) << ", "<< std::get<17>(lineorder_delta) << ", "<<
                                                            std::get<18>(lineorder_delta) << std::endl;
        }

    }

    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    std::cout<<"main end: "<< duration <<'\n';

    return 0;
}
