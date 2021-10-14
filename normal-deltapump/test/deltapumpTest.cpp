#include "BinlogParser.h"
#include <iostream>
#include <fstream>
//#include "makeTuple.h"

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
    const char* path_range = "./partitions/ranges.csv"; //range file path

    BinlogParser binlogParser;
    binlogParser.parse(path, path_range, &lineorder_record_ptr, &customer_record_ptr, &supplier_record_ptr, &part_record_ptr, &date_record_ptr);

    //write lineorder dealts to file (for test purpose)
//    std::ofstream file1;
//    file1.open("lineorder_deltas.txt", std::ios::binary);


    for(auto lineorder_pair : (*lineorder_record_ptr)){
//        std::cout<<"lineorder_pair table number: "<< lineorder_pair.first <<'\n';
        std::set<struct lineorder_record> lineorder_partition = lineorder_pair.second;
        for(auto lineorder_record : lineorder_partition){
            LineorderDelta_t lineorder_delta = lineorder_record.lineorder_delta;
//            file1.write((char*)&lineorder_delta,sizeof(lineorder_delta));
//            std::cout << std::get<0>(lineorder_delta) << ", " << std::get<1>(lineorder_delta) << ", " << std::get<2>(lineorder_delta) << ", " <<
//                    std::get<3>(lineorder_delta) << ", " << std::get<4>(lineorder_delta) << ", " << std::get<5>(lineorder_delta) << ", " <<
//                            std::get<6>(lineorder_delta) << ", " <<std::get<7>(lineorder_delta) << ", " <<std::get<8>(lineorder_delta) << ", " <<
//                                    std::get<9>(lineorder_delta) << ", " <<std::get<10>(lineorder_delta) << ", " <<std::get<11>(lineorder_delta) << ", " <<
//                                            std::get<12>(lineorder_delta) << ", " <<std::get<13>(lineorder_delta) << ", " << std::get<14>(lineorder_delta) << ", " <<
//                                                    std::get<15>(lineorder_delta) << ", "<< std::get<16>(lineorder_delta) << ", "<< std::get<17>(lineorder_delta) << ", "<<
//                                                            std::get<18>(lineorder_delta) << std::endl;
        }

    }

    for(auto customer_pair : (*customer_record_ptr)){
        std::set<struct customer_record> customer_partition = customer_pair.second;
        for(auto customer_record : customer_partition){
            CustomerDelta_t customer_delta = customer_record.customer_delta;
//            std::cout << std::get<0>(customer_delta) << ", " << std::get<1>(customer_delta) << ", " << std::get<2>(customer_delta) << ", " <<
//                      std::get<3>(customer_delta) << ", " << std::get<4>(customer_delta)  << std::endl;
        }
    }

    for(auto supplier_pair : (*supplier_record_ptr)){
        std::set<struct supplier_record> supplier_partition = supplier_pair.second;
        for(auto supplier_record : supplier_partition){
            SupplierDelta_t supplier_delta = supplier_record.supplier_delta;
//            std::cout << std::get<0>(supplier_delta) << ", " << std::get<1>(supplier_delta) << ", " << std::get<2>(supplier_delta) << ", " <<
//                      std::get<3>(supplier_delta) << ", " << std::get<4>(supplier_delta)  << std::endl;
        }
    }


//    file1.close();
//    LineorderDelta_t read_lineorder_delta;
//    std::ifstream file2;
//    file2.open("lineorder_deltas.txt",std::ios::in);
//    file2.seekg(0);
//
//    while(!file2.eof()) {
//        file2.read((char *) &read_lineorder_delta, sizeof(read_lineorder_delta));
//        std::cout << std::get<0>(read_lineorder_delta) << ", " << std::get<1>(read_lineorder_delta) << ", " << std::get<2>(read_lineorder_delta) << ", " <<
//                  std::get<3>(read_lineorder_delta) << ", " << std::get<4>(read_lineorder_delta) << ", " << std::get<5>(read_lineorder_delta) << ", " <<
//                  std::get<6>(read_lineorder_delta) << ", " <<std::get<7>(read_lineorder_delta) << ", " <<std::get<8>(read_lineorder_delta) << ", " <<
//                  std::get<9>(read_lineorder_delta) << ", " <<std::get<10>(read_lineorder_delta) << ", " <<std::get<11>(read_lineorder_delta) << ", " <<
//                  std::get<12>(read_lineorder_delta) << ", " <<std::get<13>(read_lineorder_delta) << ", " << std::get<14>(read_lineorder_delta) << ", " <<
//                  std::get<15>(read_lineorder_delta) << ", "<< std::get<16>(read_lineorder_delta) << ", "<< std::get<17>(read_lineorder_delta) << ", "<<
//                  std::get<18>(read_lineorder_delta) << std::endl;
//    }
//
//    file2.close();

    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    std::cout<<"main end: "<< duration <<'\n';

    return 0;
}
