//
// Created by ZhangOscar on 9/20/21.
//

#ifndef NORMAL_DELTAPARTITIONTABLE_H
#define NORMAL_DELTAPARTITIONTABLE_H

#include <vector>
#include <string>

namespace normal::delta_tuple {

    /**
    * Template class that holds delta table for each partition.
    * @tparam T is the tuple schema type
    */
    template <class T>
    class DeltaPartitionTable {
    public:
        // constructor for this partition table
        DeltaPartitionTable(std::vector<T>& delta_table, int partition_num, std::string& table_name);
        // constructor from deltapump
        DeltaPartitionTable();
        // transform this into a parquet file and upload to S3 ?
        void toParquet();
        // transform this into an arrow for in-memory processing
        void toArrow();

    private:
        std::vector<T> deltaTable;
        int partitionNum;
        std::string tableName;
    };

    template<class T>
    DeltaPartitionTable<T>::DeltaPartitionTable(std::vector<T> &delta_table, int partition_num, std::string &table_name) {
        deltaTable = delta_table;
        partitionNum = partition_num;
        tableName = table_name;
    }

    template<class T>
    DeltaPartitionTable<T>::DeltaPartitionTable() = default;

    template<class T>
    void DeltaPartitionTable<T>::toParquet() {

    }

    template<class T>
    void DeltaPartitionTable<T>::toArrow() {

    }



}

#endif //NORMAL_DELTAPARTITIONTABLE_H
