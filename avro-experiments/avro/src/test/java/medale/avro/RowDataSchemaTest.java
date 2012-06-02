package medale.avro;

import static org.junit.Assert.*;

import org.junit.Test;

public class RowDataSchemaTest {

	@Test
	public void test() {
		RowDataAvro rowData = new RowDataAvro();
		rowData.setColumnFamily("family");
		System.out.println(RowDataAvro.SCHEMA$);
	}

}
