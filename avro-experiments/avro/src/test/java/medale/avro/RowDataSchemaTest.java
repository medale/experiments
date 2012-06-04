package medale.avro;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

	private static final String[] VISIBILITIES = { "public", "private",
			"public,private" };
	private static final KeyValue KV1 = new KeyValue();
	private static final KeyValues KVS = new KeyValues();
	private static final RecursiveKeyValueMapRecord MAP_RECORD1 = new RecursiveKeyValueMapRecord();
	static {
		KV1.setKey("key1");
		KV1.setValue("value1");

		KVS.setKey("key2");
		List<CharSequence> values = new ArrayList<CharSequence>();
		values.add("value1");
		values.add("value2");
		KVS.setValues(values);

		Map<CharSequence, Object> map1 = new HashMap<CharSequence, Object>();
		map1.put("mapKey1", "mapValue2");
		List<CharSequence> mapValues = new ArrayList<CharSequence>();
		values.add("mapValue1");
		values.add("mapValue2");
		map1.put("mapKey2", mapValues);

		RecursiveKeyValueMapRecord mapRecord2 = new RecursiveKeyValueMapRecord();
		Map<CharSequence, Object> map2 = new HashMap<CharSequence, Object>();
		map2.put("recursiveMapKey1", "recursiveMapValue1");
		mapRecord2.setMap(map2);

		map1.put("nestedMap", mapRecord2);

		MAP_RECORD1.setMap(map1);
	}
	private static final Object[] VALUES = { KV1, KVS, MAP_RECORD1 };

	@Test
	public void testFileWriteAndRead() throws IOException {
		List<RowDataAvro> rowDataRecords = getRowDataRecords();
		SpecificDatumWriter<RowDataAvro> datumWriter = new SpecificDatumWriter<RowDataAvro>();
		DataFileWriter<RowDataAvro> fileWriter = new DataFileWriter<RowDataAvro>(
				datumWriter);
		File testFile = new File("rowDataTest.avro");
		fileWriter.create(RowDataAvro.SCHEMA$, testFile);
		for (RowDataAvro rowDataAvro : rowDataRecords) {
			fileWriter.append(rowDataAvro);
		}
		fileWriter.close();

		SpecificDatumReader<RowDataAvro> datumReader = new SpecificDatumReader<RowDataAvro>();
		DataFileReader<RowDataAvro> fileReader = new DataFileReader<RowDataAvro>(
				testFile, datumReader);
		List<RowDataAvro> actualRowDataRecords = new ArrayList<RowDataAvro>();
		while (fileReader.hasNext()) {
			RowDataAvro rowDataAvro = fileReader.next();
			actualRowDataRecords.add(rowDataAvro);
		}
		assertEquals(rowDataRecords.size(), actualRowDataRecords.size());
		for (int i = 0; i < actualRowDataRecords.size(); i++) {
			RowDataAvro actualRowData = actualRowDataRecords.get(i);
			RowDataAvro expectedRowData = rowDataRecords.get(i);
			assertEquals(expectedRowData.getRowId(), actualRowData.getRowId()
					.toString());
			assertEquals(expectedRowData.getColumnFamily(), actualRowData
					.getColumnFamily().toString());
			assertEquals(expectedRowData.getColumnQualifier(), actualRowData
					.getColumnQualifier().toString());
			assertEquals(expectedRowData.getVisibility(), actualRowData
					.getVisibility().toString());
			switch (i) {
			case 0:
				compareKeyValue(expectedRowData, actualRowData);
				break;
			case 1:
				compareKeyValues(expectedRowData, actualRowData);
				break;
			case 2:
				compareRecursiveKeyValueMapRecord(expectedRowData,
						actualRowData);
				break;
			default:
				throw new RuntimeException(
						"VALUES added without corresponding test!");
			}
		}
	}

	private void compareRecursiveKeyValueMapRecord(RowDataAvro expectedRowData,
			RowDataAvro actualRowData) {
		RecursiveKeyValueMapRecord expectedRecord = (RecursiveKeyValueMapRecord) expectedRowData.getValue();
		RecursiveKeyValueMapRecord actualRecord = (RecursiveKeyValueMapRecord) actualRowData.getValue();
		Map<CharSequence, Object> expectedMap = expectedRecord.getMap();
		Map<CharSequence, Object> actualMap = actualRecord.getMap();
		assertEquals(expectedMap.size(), actualMap.size());
		for (Map.Entry<CharSequence, Object> entry : actualMap.entrySet()) {
			System.out.println("Key: " + entry.getKey() + ": " + entry.getValue());
		}
	}

	private void compareKeyValues(RowDataAvro expectedRowData,
			RowDataAvro actualRowData) {
		KeyValues expectedKV = (KeyValues) expectedRowData.getValue();
		KeyValues actualKV = (KeyValues) actualRowData.getValue();
		assertEquals(expectedKV.getKey(), actualKV.getKey().toString());
		List<CharSequence> expectedValues = expectedKV.getValues();
		List<CharSequence> actualValues = actualKV.getValues();
		assertEquals(expectedValues.size(), actualValues.size());
		Iterator<CharSequence> expectedIterator = expectedValues.iterator();
		Iterator<CharSequence> actualIterator = actualValues.iterator();
		while(expectedIterator.hasNext()) {
			CharSequence expectedCharSeq = expectedIterator.next();
			CharSequence actualCharSeq = actualIterator.next();
			assertEquals(expectedCharSeq, actualCharSeq.toString());
		}
	}

	private void compareKeyValue(RowDataAvro expectedRowData,
			RowDataAvro actualRowData) {
		KeyValue expectedKV = (KeyValue) expectedRowData.getValue();
		KeyValue actualKV = (KeyValue) actualRowData.getValue();
		assertEquals(expectedKV.getKey(), actualKV.getKey().toString());
		assertEquals(expectedKV.getValue(), actualKV.getValue().toString());
	}

	private List<RowDataAvro> getRowDataRecords() {
		List<RowDataAvro> rowDataRecords = new ArrayList<RowDataAvro>();
		for (int i = 0; i < VALUES.length; i++) {
			RowDataAvro data = new RowDataAvro();
			int count = i + 1;
			data.setRowId("id" + count);
			data.setColumnFamily("family" + count);
			data.setColumnQualifier("qual" + count);
			data.setVisibility(VISIBILITIES[i]);
			data.setValue(VALUES[i]);
			rowDataRecords.add(data);
		}
		return rowDataRecords;
	}
}
