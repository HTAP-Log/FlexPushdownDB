#include "BinlogParser.h"


BinlogParser::BinlogParser(){
    const int kNumOptions = 3;
    JavaVMOption options[kNumOptions] = {
            {const_cast<char *>("-Xmx512m"),                                                          NULL},
            {const_cast<char *>("-verbose:gc"),                                                       NULL},
            {const_cast<char *>("-Djava.class.path=./Parser.jar:./lib/mysql-binlog-connector-java-0.25.1.jar:./lib/avro-1.10.2.jar:./lib/avro-tools-1.10.2.jar"), NULL}
    };

    JavaVMInitArgs vm_args;
    vm_args.version = JNI_VERSION_1_6;
    vm_args.options = options;
    vm_args.nOptions = sizeof(options) / sizeof(JavaVMOption);
    assert(vm_args.nOptions == kNumOptions);

//    JNIEnv *env = NULL;
//    JavaVM *jvm = NULL;
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

/////////////////////////////////////////////////////////////////////////////////////////
//helper functions to convert set to vector
void convert_to_vector_li(std::set<struct lineorder_record>* lineorder_partition_set,
                          std::vector<LineorderDelta_t>* lineorder_partition_vector){
    std::set<struct lineorder_record>::iterator it;
    for (it = lineorder_partition_set->begin(); it != lineorder_partition_set->end(); it++){
//        std::cout << "Converted " << (*it).orderkey << " " << (*it).linenumber << std::endl;
        lineorder_partition_vector->push_back((*it).lineorder_delta);
    }
}

void convert_to_vector_cu(std::set<struct customer_record>* customer_partition_set,
                          std::vector<CustomerDelta_t>* customer_partition_vector){
    std::set<struct customer_record>::iterator it;
    for (it = customer_partition_set->begin(); it != customer_partition_set->end(); it++){
        customer_partition_vector->push_back((*it).customer_delta);
    }
}

void convert_to_vector_date(std::set<struct date_record>* date_partition_set,
                            std::vector<DateDelta_t>* date_partition_vector){
    std::set<struct date_record>::iterator it;
    for (it = date_partition_set->begin(); it != date_partition_set->end(); it++){
        date_partition_vector->push_back((*it).date_delta);
    }
}

void convert_to_vector_part(std::set<struct part_record>* part_partition_set,
                            std::vector<PartDelta_t>* part_partition_vector){
    std::set<struct part_record>::iterator it;
    for (it = part_partition_set->begin(); it != part_partition_set->end(); it++){
        part_partition_vector->push_back((*it).part_delta);
    }
}

void convert_to_vector_sup(std::set<struct supplier_record>* supplier_partition_set,
                           std::vector<SupplierDelta_t>* supplier_partition_vector){
    std::set<struct supplier_record>::iterator it;
    for (it = supplier_partition_set->begin(); it != supplier_partition_set->end(); it++){
        supplier_partition_vector->push_back((*it).supplier_delta);
    }
}
/////////////////////////////////////////////////////////////////////////////////////////


void BinlogParser::parse(const char *filePath,  std::unordered_map<int, std::set<struct lineorder_record>> **lineorder_record_ptr,
                         std::unordered_map<int, std::set<struct customer_record>> **customer_record_ptr,
                         std::unordered_map<int, std::set<struct supplier_record>> **supplier_record_ptr,
                         std::unordered_map<int, std::set<struct part_record>> **part_record_ptr,
                         std::unordered_map<int, std::set<struct date_record>> **date_record_ptr,
                         Global_Map **globalMap_ptr){

    // code to call parser functions in java
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

    const jsize kNumArgs = 1;
    jclass string_cls = env->FindClass("java/lang/String");
    jobject initial_element = NULL;
    jobjectArray method_args = env->NewObjectArray(kNumArgs, string_cls, initial_element);

    jstring method_args_0 = env->NewStringUTF(filePath);
    env->SetObjectArrayElement(method_args, 0, method_args_0);

    auto byte_arr_list = static_cast<jobjectArray>(env->CallStaticObjectMethod(cls, mid, method_args_0));

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
    avro::ValidSchema lineorderSchema = loadSchema("./schemas/delta/lineorder_d.json");
    avro::ValidSchema customerSchema = loadSchema("./schemas/delta/customer_d.json");
    avro::ValidSchema supplierSchema = loadSchema("./schemas/delta/supplier_d.json");
    avro::ValidSchema partSchema = loadSchema("./schemas/delta/part_d.json");
    avro::ValidSchema dateSchema = loadSchema("./schemas/delta/date_d.json");

    //Global map to all kind of partition for 5 different tables as the result
    auto *globalMap = new Global_Map();
    //maps of partitions
    auto *lineorder_record_map = new std::unordered_map<int, std::set<struct lineorder_record>>;
//    auto *lineorder_record_map_converted = new std::unordered_map<std::string, std::vector<LineorderDelta_t>>;

    auto *customer_record_map = new std::unordered_map<int, std::set<struct customer_record>>;
//    auto *customer_record_map_converted = new std::unordered_map<std::string, std::vector<CustomerDelta_t>>;

    auto *date_record_map = new std::unordered_map<int, std::set<struct date_record>>;
//    auto *date_record_map_converted = new std::unordered_map<std::string, std::vector<DateDelta_t>>;

    auto *part_record_map = new std::unordered_map<int, std::set<struct part_record>>;
//    auto *part_record_map_converted = new std::unordered_map<std::string, std::vector<PartDelta_t>>;

    auto *supplier_record_map = new std::unordered_map<int, std::set<struct supplier_record>>;
//    auto *supplier_record_map_converted = new std::unordered_map<std::string, std::vector<SupplierDelta_t>>;

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

    //partition the lineorder table based on the range info
    std::unordered_map<int, std::set<struct lineorder_record>> li_partitions;

    // read the data input stream with the given valid schema
    while (lineorderReader.read(l1)) {
        //lineorder
//        std::cout << '(' << l1.lo_orderkey << "," << l1.lo_linenumber << ","  << l1.lo_custkey << ","  << l1.lo_partkey << ","  << l1.lo_suppkey << ","  << l1.lo_orderdate << "," <<
//                  l1.lo_orderpriority  << "," << l1.lo_shippriority << ","  << l1.lo_quantity << ","  << l1.lo_extendedprice << ","  << l1.lo_discount << ","  << l1.lo_revenue << ","  <<
//                  l1.lo_supplycost << ","  << l1.lo_tax << ","  << l1.lo_commitdate << ","  << l1.lo_shipmode << ","  << l1.type << ","  << l1.timestamp << ')' << std::endl;

        //calculate the key for the record
        int key;
        key = (int)((floor)((float)((l1).lo_orderkey) / 500)) + 1;
        //insert record into a partition (create a new partition if not exist)
        lineorder_record r = {(l1).lo_orderkey, (l1).lo_linenumber, MakeTuple::makeLineorderDeltaTuple(l1)} ;
        auto it = (*lineorder_record_map).find(key);
        if(it == (*lineorder_record_map).end()){
            std::set<struct lineorder_record> new_set;
            new_set.insert(r);
            (*lineorder_record_map).insert(std::make_pair(key, new_set));
//            std::cout << "New partition: " << key << " is created;" <<
//                      "Record inserted: " << r.orderkey <<" " << r.linenumber << std::endl;
        }
        else{
            (it->second).insert(r);
//            std::cout << "Find partition: " << key << "; " << "Record inserted: "
//                      << r.orderkey <<" " << r.linenumber << std::endl;
        }

    }

    while (customerReader.read(c1)) {
        //        std::cout << '(' << c1.c_custkey << "," << c1.c_name << ","  << c1.c_address << ","  << c1.c_city << ","  << c1.c_nation << ","  << c1.c_region << "," <<
//                  c1.c_phone  << "," << c1.c_mktsegment << ","  << c1.c_payment << ","  << c1.type << ","  << c1.timestamp << ')' << std::endl;
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
        //        std::cout << '(' << s1.s_suppkey << "," << s1.s_name << ","  << s1.s_address << ","  << s1.s_city << ","  << s1.s_nation << ","  << s1.s_region << "," <<
//                  s1.s_phone  << ","  << s1.s_ytd << ","  << s1.type << ","  << s1.timestamp << ')' << std::endl;
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

    for(auto i = (*lineorder_record_map).begin(); i != (*lineorder_record_map).end(); i++){
        std::string lineorder_prt= "lineorder.tbl." + std::to_string(i->first);
        std::vector<LineorderDelta_t> lineorder_vector;
        convert_to_vector_li(&(i->second),&lineorder_vector);
        (*globalMap)[lineorder_prt] = lineorder_vector;
//        (*lineorder_record_map_converted).insert(std::make_pair(lineorder_prt, lineorder_vector));
//        std::cout << "converted partition: " << lineorder_prt << std::endl;
    }

    for(auto i = (*customer_record_map).begin(); i != (*customer_record_map).end(); i++){
        std::string customer_prt= "customer.tbl." + std::to_string(i->first);
        std::vector<CustomerDelta_t> customer_vector;
        convert_to_vector_cu(&(i->second),&customer_vector);
        (*globalMap)[customer_prt] = customer_vector;
//        (*customer_record_map_converted).insert(std::make_pair(customer_prt, customer_vector));
    }

    for(auto i = (*date_record_map).begin(); i != (*date_record_map).end(); i++){
        std::string date_prt= "date.tbl." + std::to_string(i->first);
        std::vector<DateDelta_t> date_vector;
        convert_to_vector_date(&(i->second),&date_vector);
        (*globalMap)[date_prt] = date_vector;
//        (*date_record_map_converted).insert(std::make_pair(date_prt, date_vector));
    }

    for(auto i = (*part_record_map).begin(); i != (*part_record_map).end(); i++){
        std::string part_prt= "part.tbl." + std::to_string(i->first);
        std::vector<PartDelta_t> part_vector;
        convert_to_vector_part(&(i->second),&part_vector);
        (*globalMap)[part_prt] = part_vector;
//        (*part_record_map_converted).insert(std::make_pair(part_prt, part_vector));
    }

    for(auto i = (*supplier_record_map).begin(); i != (*supplier_record_map).end(); i++){
        std::string supplier_prt= "supplier.tbl." + std::to_string(i->first);
        std::vector<SupplierDelta_t> supplier_vector;
        convert_to_vector_sup(&(i->second),&supplier_vector);
        (*globalMap)[supplier_prt] = supplier_vector;
//        (*supplier_record_map_converted).insert(std::make_pair(supplier_prt, supplier_vector));
    }


    //Assign pointers
    *lineorder_record_ptr = lineorder_record_map;
    *customer_record_ptr = customer_record_map;
    *supplier_record_ptr = supplier_record_map;
    *part_record_ptr = part_record_map;
    *date_record_ptr = date_record_map;
    *globalMap_ptr = globalMap;


    jvm->DetachCurrentThread();

}