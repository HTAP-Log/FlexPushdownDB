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


void BinlogParser::parse(const char *filePath,  const char *rangeFilePath, std::unordered_map<int, std::set<struct lineorder_record>> **lineorder_record_ptr ){

    //get table_name, offset and range(fixed) of partitions for each table
    std::unordered_map<std::string, std::tuple<int, int>> range_result;
    std::ifstream inFile(rangeFilePath);
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


    //partition the lineorder table based on the range info
    auto lineorder_it = range_result.find("LINEORDER");
    int lineorder_offset = std::get<0>(lineorder_it->second);
    int lineorder_range = std::get<1>(lineorder_it->second);
    std::unordered_map<int, std::set<struct lineorder_record>> li_partitions;

    auto *lineorder_record_map = new std::unordered_map<int, std::set<struct lineorder_record>>;

    // read the data input stream with the given valid schema
    avro::DataFileReader <i::lineorder> lineorderReader(move(in_lineorder), lineorderSchema);
    i::lineorder l1;
    while (lineorderReader.read(l1)) {
        // calculate the key for the record
        int key;
        if (l1.lo_orderkey <= lineorder_offset) {
            key = 1;
        }else{
            key = (int)((floor)((float)(l1.lo_orderkey - lineorder_offset) / (float)lineorder_range)) + 2;
        }
        // insert record into a partition (create a new partition if not exist)
        lineorder_record r = {l1.lo_orderkey, l1.lo_linenumber, MakeTuple::makeLineorderDeltaTuple(l1)} ;
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
    /*
     * LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_SUPPKEY,LO_ORDERDATE,LO_ORDERPRIORITY,LO_SHIPPRIORITY,LO_QUANTITY,LO_EXTENDEDPRICE,LO_ORDTOTALPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE, timestamp, type
1,1,209,1552,9,"19940925","1-URGENT",0,17,2471035,11507269,4,2372193,87214,2,"19941105","TRUCK", "1", "UPDATE"
1,2,209,674,2,"19940925","1-URGENT",0,36,5668812,11507269,9,5158618,94481,6,"19941121","MAIL", "1", "UPDATE"
     * */
    // TODO: remove the hardcoded part
    i::lineorder avro_row1 = i::lineorder();
    avro_row1.lo_orderkey = 1;
    avro_row1.lo_linenumber = 1;
    avro_row1.lo_custkey = 209;
    avro_row1.lo_partkey = 1552;
    avro_row1.lo_suppkey = 9;
    avro_row1.lo_orderdate = 19940925;
    avro_row1.lo_orderpriority = "1-URGENT";
    avro_row1.lo_shippriority = "0";
    avro_row1.lo_quantity = 17;
    avro_row1.lo_extendedprice = 2471035;
    avro_row1.lo_ordtotalprice = 11507269;
    avro_row1.lo_discount = 4;
    avro_row1.lo_revenue = 2372193;
    avro_row1.lo_supplycost = 87214;
    avro_row1.lo_tax = 2;
    avro_row1.lo_commitdate = 19941105;
    avro_row1.lo_shipmode = "TRUCK";
    avro_row1.timestamp = 1;
    avro_row1.type = "UPDATE";

    i::lineorder avro_row2 = i::lineorder();
    avro_row2.lo_orderkey = 1;
    avro_row2.lo_linenumber = 2;
    avro_row2.lo_custkey = 209;
    avro_row2.lo_partkey = 674;
    avro_row2.lo_suppkey = 2;
    avro_row2.lo_orderdate = 19940925;
    avro_row2.lo_orderpriority = "1-URGENT";
    avro_row2.lo_shippriority = "0";
    avro_row2.lo_quantity = 36;
    avro_row2.lo_extendedprice = 2471035;
    avro_row2.lo_ordtotalprice = 11507269;
    avro_row2.lo_discount = 4;
    avro_row2.lo_revenue = 2372193;
    avro_row2.lo_supplycost = 87214;
    avro_row2.lo_tax = 2;
    avro_row2.lo_commitdate = 19941105;
    avro_row2.lo_shipmode = "MAIL";
    avro_row2.timestamp = 1;
    avro_row2.type = "UPDATE";
    struct lineorder_record record1 = {
            1,
            1,
            MakeTuple::makeLineorderDeltaTuple(avro_row1)
    };
    struct lineorder_record record2 = {
            1,
            2,
            MakeTuple::makeLineorderDeltaTuple(avro_row1)
    };
    std::set<struct lineorder_record> temp_records {record1, record2};
    auto *temp_set = new std::unordered_map<int, std::set<struct lineorder_record>>;
    temp_set->insert(std::make_pair(0, temp_records)); // partition 0 -> 2 hardcoded records
    *lineorder_record_ptr = temp_set;

    // TODO: uncomment this
    // *lineorder_record_ptr = lineorder_record_map;
    jvm->DetachCurrentThread();

}