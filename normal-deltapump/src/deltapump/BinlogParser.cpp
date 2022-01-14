#include "BinlogParser.h"
#include <filesystem>
using namespace std::filesystem;

//TODO: change /home/ubuntu/pushdown_db_e2e/cmake-build-remote-release to your working directory when testing
BinlogParser::BinlogParser(){
    const int kNumOptions = 3;
    JavaVMOption options[kNumOptions] = {
//            {const_cast<char *>("-Xmx512m"),NULL},
//            {const_cast<char *>("-verbose:gc"),NULL},
//            {const_cast<char *>("-Djava.class.path="
//                                "/home/ubuntu/Han/htap-e2e/cmake-build-remote-debug/normal-deltapump/Parser.jar:"
//                                "/home/ubuntu/Han/htap-e2e/cmake-build-remote-debug/normal-deltapump/lib/mysql-binlog-connector-java-0.25.1.jar:"
//                                "/home/ubuntu/Han/htap-e2e/cmake-build-remote-debug/normal-deltapump/lib/avro-1.10.2.jar:"
//                                "/home/ubuntu/Han/htap-e2e/cmake-build-remote-debug/normal-deltapump//lib/avro-tools-1.10.2.jar"),
//                    NULL}};
            {const_cast<char *>("-Xmx512m"),                                                          NULL},
            {const_cast<char *>("-verbose:gc"),                                                       NULL},
            {const_cast<char *>("-Djava.class.path=../normal-deltapump/Parser.jar:../normal-deltapump/lib/mysql-binlog-connector-java-0.25.1.jar:../normal-deltapump/lib/avro-1.10.2.jar:../normal-deltapump/lib/avro-tools-1.10.2.jar"), NULL}

    };

    JavaVMInitArgs vm_args;
    vm_args.version = JNI_VERSION_1_6;
    vm_args.options = options;
    vm_args.nOptions = sizeof(options) / sizeof(JavaVMOption);
    assert(vm_args.nOptions == kNumOptions);

    int res = JNI_CreateJavaVM(&jvm, reinterpret_cast<void **>(&g_env), &vm_args);
    if (res != JNI_OK) {
        std::cerr << "FAILED: JNI_CreateJavaVM " << res << std::endl;
        exit(-1);
    }
}

avro::ValidSchema loadSchema(const char* filename)
{
    std::ifstream ifs(filename);
    avro::ValidSchema result;
    avro::compileJsonSchema(ifs, result);
    return result;
}


void BinlogParser::parse(const char *filePath,  std::unordered_map<int, std::set<struct lineorder_record>> **lineorder_record_ptr,
                         std::unordered_map<int, std::set<struct customer_record>> **customer_record_ptr,
                         std::unordered_map<int, std::set<struct supplier_record>> **supplier_record_ptr,
                         std::unordered_map<int, std::set<struct part_record>> **part_record_ptr,
                         std::unordered_map<int, std::set<struct date_record>> **date_record_ptr){

    if (jvm == NULL) {
        std::cerr << "FAILED: jvm not initialized" << std::endl;
        exit(-1);
    }

    JNIEnv *env = NULL;
    jvm->AttachCurrentThread((void **) &env, NULL);

    const char *kClassName = "binlog_parser/Parser";
    jclass cls = env->FindClass(kClassName);
    if (cls == NULL) {
        std::cerr << "FAILED: FindClass" << std::endl;
        exit(-1);
    }

    const char *kMethodName = "parseBinlogFile";
    jmethodID mid =
            env->GetStaticMethodID(cls, kMethodName, "(Ljava/lang/String;)[[B");
    if (mid == NULL) {
        std::cerr << "FAILED: GetStaticMethodID" << std::endl;
        exit(-1);
    }

    // configure input parameter
    const jsize kNumArgs = 1;
    jclass string_cls = env->FindClass("java/lang/String");
    jobject initial_element = NULL;
    jobjectArray method_args = env->NewObjectArray(kNumArgs, string_cls, initial_element);

    jstring method_args_0 = env->NewStringUTF(filePath);
    env->SetObjectArrayElement(method_args, 0, method_args_0);

    //call API in Parser.java to parse binlog
    auto byte_arr_list = static_cast<jobjectArray>(env->CallStaticObjectMethod(cls, mid, method_args_0));
    /*if(byte_arr_list == NULL ){
        std::cerr << "FAILED: byte_arr_list is NULL";
        exit(1);
    }*/

    // extract serialized byte array for each table
    auto jlineorder = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 0);
    jsize lineorder_dim = jlineorder ? env->GetArrayLength(jlineorder) : 0;

    auto jcustomer = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 1);
    jsize customer_dim = jcustomer ? env->GetArrayLength(jcustomer) : 0;

    auto jsupplier = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 2);
    jsize supplier_dim = jsupplier ? env->GetArrayLength(jsupplier) : 0;

    auto jpart = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 3);
    jsize part_dim = jpart ? env->GetArrayLength(jpart) : 0;

    auto jdate = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 4);
    jsize date_dim = jdate ? env->GetArrayLength(jdate) : 0;

    // cast ti cpp byte array type
    jbyte *lineorder = env->GetByteArrayElements(jlineorder, 0);
    const auto* input_lineorder = (const uint8_t*) lineorder;

    jbyte *customer = env->GetByteArrayElements(jcustomer, 0);
    const auto* input_customer = (const uint8_t*) customer;

    jbyte *supplier = env->GetByteArrayElements(jsupplier, 0);
    const auto* input_supplier = (const uint8_t*) supplier;

    jbyte *part = env->GetByteArrayElements(jpart, 0);
    const auto* input_part = (const uint8_t*) part;

    jbyte *date = env->GetByteArrayElements(jdate, 0);
    const auto* input_date = (const uint8_t*) date;


    // put data into input streams
    std::unique_ptr<avro::InputStream> in_lineorder = avro::memoryInputStream(input_lineorder, (int) lineorder_dim);
    std::unique_ptr<avro::InputStream> in_customer = avro::memoryInputStream(input_customer, (int) customer_dim);
    std::unique_ptr<avro::InputStream> in_supplier = avro::memoryInputStream(input_supplier, (int) supplier_dim);
    std::unique_ptr<avro::InputStream> in_part = avro::memoryInputStream(input_part, (int) part_dim);
    std::unique_ptr<avro::InputStream> in_date = avro::memoryInputStream(input_date, (int) date_dim);

    // load schemas
//    avro::ValidSchema lineorderSchema = loadSchema("/home/ubuntu/Han/htap-e2e/normal-deltapump/include/deltapump/schemas/delta/lineorder_d.json");
//    avro::ValidSchema customerSchema = loadSchema("/home/ubuntu/Han/htap-e2e/normal-deltapump/include/deltapump/schemas/delta/customer_d.json");
//    avro::ValidSchema supplierSchema = loadSchema("/home/ubuntu/Han/htap-e2e/normal-deltapump/include/deltapump/schemas/delta/supplier_d.json");
//    avro::ValidSchema partSchema = loadSchema("/home/ubuntu/Han/htap-e2e/normal-deltapump/include/deltapump/schemas/delta/part_d.json");
//    avro::ValidSchema dateSchema = loadSchema("/home/ubuntu/Han/htap-e2e/normal-deltapump/include/deltapump/schemas/delta/date_d.json");
    avro::ValidSchema lineorderSchema = loadSchema("../../normal-deltapump/include/deltapump/schemas/delta/lineorder_d.json");
    avro::ValidSchema customerSchema = loadSchema("../../normal-deltapump/include/deltapump/schemas/delta/customer_d.json");
    avro::ValidSchema supplierSchema = loadSchema("../../normal-deltapump/include/deltapump/schemas/delta/supplier_d.json");
    avro::ValidSchema partSchema = loadSchema("../../normal-deltapump/include/deltapump/schemas/delta/part_d.json");
    avro::ValidSchema dateSchema = loadSchema("../../normal-deltapump/include/deltapump/schemas/delta/date_d.json");


    //maps of partitions
    auto *lineorder_record_map = new std::unordered_map<int, std::set<struct lineorder_record>>;
    auto *customer_record_map = new std::unordered_map<int, std::set<struct customer_record>>;
    auto *date_record_map = new std::unordered_map<int, std::set<struct date_record>>;
    auto *part_record_map = new std::unordered_map<int, std::set<struct part_record>>;
    auto *supplier_record_map = new std::unordered_map<int, std::set<struct supplier_record>>;

    // read the data input stream with the given valid schema
    avro::DataFileReader <i::lineorder> lineorderReader(move(in_lineorder), lineorderSchema);
    i::lineorder l1;

    avro::DataFileReader <i::customer> customerReader(move(in_customer), customerSchema);
    i::customer c1;

    avro::DataFileReader <i::date> dateReader(move(in_date), dateSchema);
    i::date d1;

    avro::DataFileReader <i::part> partReader(move(in_part), partSchema);
    i::part p1;

    avro::DataFileReader <i::supplier> supplierReader(move(in_supplier), supplierSchema);
    i::supplier s1;

    //get table_name, offset and range(fixed) of partitions for each table
    std::unordered_map<std::string, std::tuple<int, int>> range_result;


    std::ifstream inFile("../../normal-deltapump/include/deltapump/rangeFile/globalMins");
    if(!inFile.is_open()) throw std::runtime_error("Could not open range file");
    std::string lineStr, table_name;
    int offset, fixed_range;
    while(std::getline(inFile, lineStr)){
        std::stringstream ss(lineStr);
        std::string str;
        std::vector<std::string> lineArray;
        while(std::getline(ss, str, ',')){
            lineArray.push_back(str);
        }
        table_name = lineArray.at(0);
        offset = std::stoi(lineArray.at(1));
        fixed_range = std::stoi(lineArray.at(2));
        std::tuple<int, int> line_tuple;
        line_tuple = std::make_tuple(offset, fixed_range);
        range_result.insert(std::make_pair(table_name, line_tuple));
    }
    inFile.close();
    auto lineorder_it = range_result.find("lineorder");
    int lineorder_offset = std::get<0>(lineorder_it->second);
    int lineorder_range = std::get<1>(lineorder_it->second);

    auto customer_it = range_result.find("customer");
    int customer_offset = std::get<0>(customer_it->second);
    int customer_range = std::get<1>(customer_it->second);

    auto date_it = range_result.find("date");
    int date_offset = std::get<0>(date_it->second);
    int date_range = std::get<1>(date_it->second);

    auto part_it = range_result.find("part");
    int part_offset = std::get<0>(part_it->second);
    int part_range = std::get<1>(part_it->second);

    auto supplier_it = range_result.find("supplier");
    int supplier_offset = std::get<0>(supplier_it->second);
    int supplier_range = std::get<1>(supplier_it->second);

    // read the data input stream with the given valid schema
    while (lineorderReader.read(l1)) {
        //calculate the key for the record
        int key;
        if ((l1).lo_orderkey <= lineorder_offset) {
            key = 0;
        } else{
            key = (int)((floor)((float)((l1).lo_orderkey - lineorder_offset) / lineorder_range));
        }
        //insert record into a partition (create a new partition if not exist)
        lineorder_record r = {(l1).lo_orderkey, (l1).lo_linenumber, MakeTuple::makeLineorderDeltaTuple(l1)} ;
        auto it = (*lineorder_record_map).find(key);
        if(it == (*lineorder_record_map).end()){
            std::set<struct lineorder_record> new_set;
            new_set.insert(r);
            (*lineorder_record_map).insert(std::make_pair(key, new_set));
        }
        else{
            (it->second).insert(r);
        }

    }


     //TODO: partitioning process for other tables in later E2Etesting
    while (customerReader.read(c1)) {
        int key;
        key = (int)((floor)((float)((c1).c_custkey) / 500)) + 1;
        customer_record r = {(c1).c_custkey, MakeTuple::makeCustomerDeltaTuple(c1)} ;
        auto it = (*customer_record_map).find(key);
        if(it == (*customer_record_map).end()){
            std::set<struct customer_record> new_set;
            new_set.insert(r);
            (*customer_record_map).insert(std::make_pair(key, new_set));
        }
        else{
            (it->second).insert(r);
        }
    }

    while (dateReader.read(d1)) {
        int key;
        key = (int)((floor)((float)((d1).d_datekey) / 500)) + 1;
        date_record r = {(d1).d_datekey, MakeTuple::makeDateDeltaTuple(d1)} ;
        auto it = (*date_record_map).find(key);
        if(it == (*date_record_map).end()){
            std::set<struct date_record> new_set;
            new_set.insert(r);
            (*date_record_map).insert(std::make_pair(key, new_set));
        }
        else{
            (it->second).insert(r);
        }
    }

    while (partReader.read(p1)) {
        int key;
        key = (int)((floor)((float)((p1).p_partkey) / 500)) + 1;
        part_record r = {(p1).p_partkey, MakeTuple::makePartDeltaTuple(p1)} ;
        auto it = (*part_record_map).find(key);
        if(it == (*part_record_map).end()){
            std::set<struct part_record> new_set;
            new_set.insert(r);
            (*part_record_map).insert(std::make_pair(key, new_set));
        }
        else{
            (it->second).insert(r);
        }
    }

    while (supplierReader.read(s1)) {
        int key;
        key = (int)((floor)((float)((s1).s_suppkey) / 500)) + 1;
        supplier_record r = {(s1).s_suppkey, MakeTuple::makeSupplierDeltaTuple(s1)} ;
        auto it = (*supplier_record_map).find(key);
        if(it == (*supplier_record_map).end()){
            std::set<struct supplier_record> new_set;
            new_set.insert(r);
            (*supplier_record_map).insert(std::make_pair(key, new_set));
        }
        else{
            (it->second).insert(r);
        }
    }

    //Assign pointers
    *lineorder_record_ptr = lineorder_record_map;
    *customer_record_ptr = customer_record_map;
    *supplier_record_ptr = supplier_record_map;
    *part_record_ptr = part_record_map;
    *date_record_ptr = date_record_map;

    jvm->DetachCurrentThread();

}