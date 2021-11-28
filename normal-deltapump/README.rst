===========
Normal Deltapump
===========

Binlog Parser

Change paths to your working directory in following locations before running:

Parser.java:
1.
    Schema lineorderSchema = new Schema.Parser().parse(new File("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/lineorder_d.json"));
    Schema customerSchema = new Schema.Parser().parse(new File("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/customer_d.json"));
    Schema supplierSchema = new Schema.Parser().parse(new File("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/supplier_d.json"));
    Schema partSchema = new Schema.Parser().parse(new File("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/part_d.json"));
    Schema dateSchema = new Schema.Parser().parse(new File("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/date_d.json"));

BinlogParser.cpp:
1.
    JavaVMOption options[kNumOptions] = {
            {const_cast<char *>("-Xmx512m"),                                                          NULL},
            {const_cast<char *>("-verbose:gc"),                                                       NULL},
            {const_cast<char *>("-Djava.class.path=/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/Parser.jar:/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/lib/mysql-binlog-connector-java-0.25.1.jar:/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/lib/avro-1.10.2.jar:/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/lib/avro-tools-1.10.2.jar"), NULL}
    };

2.
    avro::ValidSchema lineorderSchema = loadSchema("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/lineorder_d.json");
    avro::ValidSchema customerSchema = loadSchema("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/customer_d.json");
    avro::ValidSchema supplierSchema = loadSchema("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/supplier_d.json");
    avro::ValidSchema partSchema = loadSchema("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/part_d.json");
    avro::ValidSchema dateSchema = loadSchema("/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/schemas/delta/date_d.json");

GetTailDeltas.cpp (normal-htap/src/deltamanager):
1.
    const char* path = "/home/ubuntu/pushdown_db_temp_e2e/cmake-build-remote-debug/normal-deltapump/bin.000002"; //binlog file path