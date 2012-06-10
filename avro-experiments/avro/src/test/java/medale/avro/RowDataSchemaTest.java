package medale.avro;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

public class RowDataSchemaTest {

	private static final String VISIBILITY = "admin,dev";
	private static final KeyValue KV1 = new KeyValue();
	private static final KeyValues KVS = new KeyValues();
	private static final NestableKeyValueMapRecord MAP_RECORD1 = new NestableKeyValueMapRecord();
	static {
		KV1.setKey("key1");
		KV1.setValue("value1");

		KVS.setKey("key2");
		List<String> values = new ArrayList<String>();
		values.add("value1");
		values.add("value2");
		KVS.setValues(values);

		Map<String, Object> map1 = new HashMap<String, Object>();
		map1.put("mapKey1", "mapValue2");
		List<String> mapValues = new ArrayList<String>();
		values.add("mapValue1");
		values.add("mapValue2");
		map1.put("mapKey2", mapValues);

		NestableKeyValueMapRecord mapRecord2 = new NestableKeyValueMapRecord();
		Map<String, Object> map2 = new HashMap<String, Object>();
		map2.put("recursiveMapKey1", "recursiveMapValue1");
		mapRecord2.setMap(map2);

		map1.put("nestedMap", mapRecord2);

		MAP_RECORD1.setMap(map1);
	}
	private static final Object[] VALUES = { KV1, KVS, MAP_RECORD1 };

	@Test
	public void testFileWriteAndRead() throws IOException {
		RowDataAvro expectedRowData = getRowData();
		SpecificDatumWriter<RowDataAvro> datumWriter = new SpecificDatumWriter<RowDataAvro>();
		DataFileWriter<RowDataAvro> fileWriter = new DataFileWriter<RowDataAvro>(
				datumWriter);
		File testFile = new File("rowDataTest.avro");
		fileWriter.create(RowDataAvro.SCHEMA$, testFile);
		fileWriter.append(expectedRowData);
		fileWriter.close();

		SpecificDatumReader<RowDataAvro> datumReader = new SpecificDatumReader<RowDataAvro>();
		DataFileReader<RowDataAvro> fileReader = new DataFileReader<RowDataAvro>(
				testFile, datumReader);
		RowDataAvro actualRowData = fileReader.next();
		assertEquals(expectedRowData.getRowId(), actualRowData.getRowId()
				.toString());
		assertEquals(expectedRowData.getColumnFamily(), actualRowData
				.getColumnFamily().toString());
		assertEquals(expectedRowData.getColumnQualifier(), actualRowData
				.getColumnQualifier().toString());
		assertEquals(expectedRowData.getVisibility(), actualRowData
				.getVisibility().toString());
		List<Object> expectedValueList = expectedRowData.getValue();
		List<Object> actualValueList = actualRowData.getValue();
		assertEquals(expectedValueList.size(), actualValueList.size());
		Iterator<Object> expectedIterator = expectedValueList.iterator();
		Iterator<Object> actualIterator = actualValueList.iterator();
		while(expectedIterator.hasNext()) {
			Object expectedValue = expectedIterator.next();
			Object actualValue = actualIterator.next();
			compare(expectedValue, actualValue);
		}
	}

	private void compare(Object expectedValue, Object actualValue) {
		if (expectedValue instanceof NestableKeyValueMapRecord) {
			compareNestableKeyValueMapRecord(expectedValue, actualValue);
		} else {
			assertEquals(expectedValue, actualValue);
		}
	}

	private void compareNestableKeyValueMapRecord(Object expectedValue, Object actualValue) {
		NestableKeyValueMapRecord expectedRecord = (NestableKeyValueMapRecord) expectedValue;
		NestableKeyValueMapRecord actualRecord = (NestableKeyValueMapRecord) actualValue;
		Map<String, Object> expectedMap = expectedRecord.getMap();
		Map<String, Object> actualMap = actualRecord.getMap();
		assertEquals(expectedMap.size(), actualMap.size());
		for (String expectedKey : expectedMap.keySet()) {
			Object currExpectedValue = expectedMap.get(expectedKey);
			Object currActualValue = actualMap.get(expectedKey);
			if (currExpectedValue instanceof NestableKeyValueMapRecord) {
				compareNestableKeyValueMapRecord(currExpectedValue, currActualValue);
			}
		}
	}
	
	private RowDataAvro getRowData() {
		RowDataAvro data = new RowDataAvro();
		data.setRowId("id");
		data.setColumnFamily("family");
		data.setColumnQualifier("qual");
		data.setVisibility(VISIBILITY);
		data.setValue(Arrays.asList(VALUES));

		return data;
	}
}
