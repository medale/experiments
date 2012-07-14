package medale.avro;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

public class RowDataTest {
	@Test
	public void test() throws IOException {
		//write row data without Avro header and read it back in.
		String rowId = "id";
		String columnFamily = "cf";
		String columnQualifier = "cq";
		
		RowData rowData = new RowData();
		rowData.setRowId(rowId);
		rowData.setColumnFamily(columnFamily);
		rowData.setColumnQualifier(columnQualifier);
		RowDataValue value = new RowDataValue();
		Type type = new Type();
		type.setTag("/car");
		type.setVersion(1);
		TypeValuePair valuePair = new TypeValuePair();
		valuePair.setType(type);
		valuePair.setValue("BMW");
		List<Object> rowItems = new ArrayList<Object>();
		rowItems.add(valuePair);
		value.setRowItems(rowItems);
		rowData.setValue(value);
		
		File avroFile = new File("noSchema.avro");
		FileOutputStream fout = new FileOutputStream(avroFile);
		EncoderFactory eFactory = new EncoderFactory();
		BinaryEncoder encoder = eFactory.binaryEncoder(fout, null);
		SpecificDatumWriter<RowData> datumWriter = new SpecificDatumWriter<RowData>();
		datumWriter.setSchema(RowData.SCHEMA$);
		datumWriter.write(rowData, encoder);
		encoder.flush();
		fout.close();
		
		FileInputStream fin = new FileInputStream(avroFile);
		SpecificDatumReader<RowData> datumReader = new SpecificDatumReader<RowData>();
		datumReader.setSchema(RowData.SCHEMA$);
		DecoderFactory dFactory = new DecoderFactory();
		Decoder decoder = dFactory.binaryDecoder(fin, null);
		RowData actualRowData = datumReader.read(null, decoder);
		System.out.println(actualRowData);
		assertEquals(rowData, actualRowData);
	}

}
