package binlog_parser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

public class Parser {
		public static String currentTable = null;
    	public static long last_timestamp = 0; //timestamp of last log event in previous call
    	private static boolean firstCall = true; //whether the deltapump is called for the first time
        // binlog file reader
    	private static File binlogFile = new File("../../normal-deltapump/include/deltapump/logs/bin.000002");
        private static EventDeserializer eventDeserializer = new EventDeserializer();
        private static BufferedInputStream bf;
        private static BinaryLogFileReader reader;
        static {
            try{
                bf = new BufferedInputStream(new FileInputStream(binlogFile));
                reader = new BinaryLogFileReader(bf, eventDeserializer);
            } catch (IOException e){
                e.printStackTrace();
            }
        }

	/**
	 * parse the given binlog file and write all event info in the format of avro, added field in schema compared to HaTrickBench
	 * @param binlogFilePath
	 * @return serialized avro byte array; order of tables in the vector: lineorder, customer, supplier, part, date
	 * @throws IOException
	 */
	private static byte [][] parseBinlogFile(String binlogFilePath) throws IOException {
	    long counter = 0;
	    long number_of_writes = 0;
        long number_of_deletes = 0;
        long number_of_updates = 0;
// 		File binlogFile = new File(binlogFilePath);

		double startTime = System.currentTimeMillis();

		byte[][] output = new byte[5][];

		ByteArrayOutputStream lineorderOutputStream = new ByteArrayOutputStream();
		ByteArrayOutputStream customerOutputStream = new ByteArrayOutputStream();
		ByteArrayOutputStream supplierOutputStream = new ByteArrayOutputStream();
		ByteArrayOutputStream partOutputStream = new ByteArrayOutputStream();
		ByteArrayOutputStream dateOutputStream = new ByteArrayOutputStream();

        String[] lineorderCol = {"lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey", "lo_suppkey",
                                "lo_orderdate","lo_orderpriority","lo_shippriority", "lo_quantity",
                                "lo_extendedprice", "lo_ordtotalprice", "lo_discount", "lo_revenue", "lo_supplycost",
                                "lo_tax", "lo_commitdate", "lo_shipmode", "type", "timestamp"};

		String[] customerCol = {"c_custkey", "c_name", "c_address", "c_city", "c_nation",
		                        "c_region", "c_phone", "c_mktsegment", "c_payment", "type", "timestamp"};
		String[] supplierCol = {"s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region",
		                        "s_phone", "s_ytd", "type", "timestamp"};
		String[] partCol = {"p_partkey", "p_name", "p_mfgr", "p_category", "p_brand1", "p_color",
		                    "p_type", "p_size", "p_container", "type", "timestamp"};
		String[] dateCol = {"d_datekey", "d_date", "d_dayofweek", "d_month", "d_year", "d_yearmonthnum", "d_yearmonth",
		                    "d_daynuminweek", "d_daynuminmonth", "d_daynuminyear", "d_monthnuminyear",
		                    "d_weeknuminyear", "d_sellingseason", "d_lastdayinweekfl",  "d_lastdayinmonthfl",
		                    "d_holidayfl", "d_weekdayfl", "type", "timestamp"};

		//First, we use a Parser to read our schema definition and create a Schema object.
		Schema lineorderSchema = new Schema.Parser().parse(new File("../../normal-deltapump/include/deltapump/schemas/delta/lineorder_d.json"));
		Schema customerSchema = new Schema.Parser().parse(new File("../../normal-deltapump/include/deltapump/schemas/delta/customer_d.json"));
		Schema supplierSchema = new Schema.Parser().parse(new File("../../normal-deltapump/include/deltapump/schemas/delta/supplier_d.json"));
		Schema partSchema = new Schema.Parser().parse(new File("../../normal-deltapump/include/deltapump/schemas/delta/part_d.json"));
		Schema dateSchema = new Schema.Parser().parse(new File("../../normal-deltapump/include/deltapump/schemas/delta/date_d.json"));


		//avro serializer
		DatumWriter<GenericRecord> lineorderDatumWriter = new GenericDatumWriter<GenericRecord>(lineorderSchema);
		DatumWriter<GenericRecord> customerDatumWriter = new GenericDatumWriter<GenericRecord>(customerSchema);
		DatumWriter<GenericRecord> supplierDatumWriter = new GenericDatumWriter<GenericRecord>(supplierSchema);
		DatumWriter<GenericRecord> partDatumWriter = new GenericDatumWriter<GenericRecord>(partSchema);
		DatumWriter<GenericRecord> dateDatumWriter = new GenericDatumWriter<GenericRecord>(dateSchema);

		DataFileWriter<GenericRecord> lineorderFileWriter = new DataFileWriter<GenericRecord>(lineorderDatumWriter);
		DataFileWriter<GenericRecord> customerFileWriter = new DataFileWriter<GenericRecord>(customerDatumWriter);
		DataFileWriter<GenericRecord> partFileWriter = new DataFileWriter<GenericRecord>(partDatumWriter);
		DataFileWriter<GenericRecord> dateFileWriter = new DataFileWriter<GenericRecord>(dateDatumWriter);
		DataFileWriter<GenericRecord> supplierFileWriter = new DataFileWriter<GenericRecord>(supplierDatumWriter);

		lineorderFileWriter.create(lineorderSchema, lineorderOutputStream);
		customerFileWriter.create(customerSchema, customerOutputStream);
		partFileWriter.create(partSchema, partOutputStream);
		dateFileWriter.create(dateSchema, dateOutputStream);
		supplierFileWriter.create(supplierSchema, supplierOutputStream);

		HashMap<String, Schema> schemaList = new HashMap<String, Schema>();
		HashMap<String, String[]> colList = new HashMap<String, String[]>();
		HashMap<String, DatumWriter<GenericRecord>> dwList = new HashMap<String, DatumWriter<GenericRecord>>();
		HashMap<String, ByteArrayOutputStream> osList = new HashMap<String, ByteArrayOutputStream>();
		HashMap<String, DataFileWriter<GenericRecord>> fwList = new HashMap<String, DataFileWriter<GenericRecord>>();

		//put schemas and file writer into hash map
		//NOTE: in HATtrick benchmark all table names are in uppercase,
		//may need to modify code when encounter lowercase table names (e.g. currentTable.toLowerCase())
		schemaList.put("LINEORDER", lineorderSchema);
		schemaList.put("CUSTOMER", customerSchema);
		schemaList.put("SUPPLIER", supplierSchema);
		schemaList.put("PART", partSchema);
		schemaList.put("DATE", dateSchema);

		colList.put("LINEORDER", lineorderCol);
		colList.put("CUSTOMER", customerCol);
		colList.put("SUPPLIER", supplierCol);
		colList.put("PART", partCol);
		colList.put("DATE", dateCol);

		dwList.put("LINEORDER", lineorderDatumWriter);
		dwList.put("CUSTOMER", customerDatumWriter);
		dwList.put("SUPPLIER", supplierDatumWriter);
		dwList.put("PART", partDatumWriter);
		dwList.put("DATE", dateDatumWriter);

		osList.put("LINEORDER", lineorderOutputStream);
		osList.put("CUSTOMER", customerOutputStream);
		osList.put("SUPPLIER", supplierOutputStream);
		osList.put("PART", partOutputStream);
		osList.put("DATE", dateOutputStream);

		fwList.put("LINEORDER", lineorderFileWriter);
		fwList.put("CUSTOMER", customerFileWriter);
		fwList.put("SUPPLIER", supplierFileWriter);
		fwList.put("PART", partFileWriter);
		fwList.put("DATE", dateFileWriter);


// 		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
				EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
// 		BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer);

		try {
			for (Event event; (event = reader.readEvent()) != null;) {
				EventHeaderV4 header = event.getHeader();
				EventType type = header.getEventType();
				long timestamp = header.getTimestamp();

                if(firstCall){
                    last_timestamp = timestamp;
                    firstCall = false;
                }


				if (type == EventType.TABLE_MAP) {
					currentTable = ((TableMapEventData) event.getData()).getTable();
					continue;
				}

                if(currentTable == null)
                    continue;

                // Get for currentTable record, dataFileWriter, colList and datumWriter.
				GenericRecord record = new GenericData.Record(schemaList.get(currentTable));
				DataFileWriter<GenericRecord> dataFileWriter = fwList.get(currentTable);
				String[] colName = colList.get(currentTable);
				DatumWriter<GenericRecord> datumWriter = dwList.get(currentTable);

				if (EventType.isWrite(type)) {
					WriteRowsEventData data = (WriteRowsEventData) event.getData();
					List<Serializable[]> list = data.getRows();

					//for every row in the list, put each row into one string array
					for (Serializable[] row : list) {
						int writeIndex = 0;
						for (Serializable val : row) {
							String value;
							if (val instanceof byte[]) {
								value = new String((byte[]) val, StandardCharsets.UTF_8);
								record.put(colName[writeIndex++], value);
							} else {
								record.put(colName[writeIndex++], val);
							}
						}
						record.put(colName[writeIndex++], type.toString());
						record.put(colName[writeIndex++], timestamp);
					}
					number_of_writes++;
				} else if (EventType.isDelete(type)) {
					DeleteRowsEventData data = (DeleteRowsEventData) event.getData();
					List<Serializable[]> list = data.getRows();
					// TODO: handle deletes
					for (Serializable[] row : list) {
						int deleteIndex = 0;
						for (Serializable val : row) {
							String value;
							if (val instanceof byte[]) {
								value = new String((byte[]) val, StandardCharsets.UTF_8);
								record.put(colName[deleteIndex++], value);
							} else {
								record.put(colName[deleteIndex++], val);
							}
						}
						record.put(colName[deleteIndex++], type.toString());
						record.put(colName[deleteIndex++], timestamp);
					}
					number_of_deletes++;
				} else if (EventType.isUpdate(type)) {
					UpdateRowsEventData data = (UpdateRowsEventData) event.getData();
					Long tableId = data.getTableId();
					List<Map.Entry<Serializable[], Serializable[]>> list = data.getRows();
					for (Map.Entry<Serializable[], Serializable[]> entry : list) {
						Serializable[] row = entry.getValue();
						int updateIndex = 0;
						for (Serializable val : row) {
							String value;
							if (val instanceof byte[]) {
								value = new String((byte[]) val, StandardCharsets.UTF_8);
								record.put(colName[updateIndex++], value);
							} else {
								record.put(colName[updateIndex++], val);
							}
						}
						record.put(colName[updateIndex++], type.toString());
						record.put(colName[updateIndex++], timestamp);
					}
					number_of_updates++;
				} else {
					continue;
				}
				dataFileWriter.append(record);
				counter++;

                // every time deltapump is called, it sends next 5 seconds transaction starting from the previous position
				if(timestamp >= (last_timestamp + 1 * 1 * 500)){
				    last_timestamp = timestamp;
				    break;
				}
			}
			double endTime = System.currentTimeMillis();
			double duration = (endTime - startTime) / 1000;
			/*System.out.println("Process time: " + duration + " s");
			System.out.println("Number of events: " + counter);
			System.out.println("Number of wrtites: " + number_of_writes);
			System.out.println("Number of deletes: " + number_of_deletes);
			System.out.println("Number of updates: " + number_of_updates);*/
		} catch (Exception e) {
			//e.printStackTrace();
		} finally {
			for(ByteArrayOutputStream o : osList.values()) {
				if(o != null) {
					o.close();
				}
			}
// 			reader.close();
		}

		//if no event for certain table, the length of its output stream would be 0
		output[0] = lineorderOutputStream.toByteArray();
		output[1] = customerOutputStream.toByteArray();
		output[2] = supplierOutputStream.toByteArray();
		output[3] = partOutputStream.toByteArray();
		output[4] = dateOutputStream.toByteArray();

		return output;

	}

}