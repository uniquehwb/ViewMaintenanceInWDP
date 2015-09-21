package de.webdataplatform.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ClientUpdates {
	
	private String prefix;
	private HTable baseTable;
	private int updateIndex;
	
	public ClientUpdates(String prefix, HTable baseTable, int updateIndex) {
		this.prefix = prefix;
		this.baseTable = baseTable;
		this.updateIndex = updateIndex;
	}
	
	public void start() throws IOException {
		String rowKey1;
		String rowKey2;
		String rowKey3;
		String rowKey4;
		
		Put put1;
		Put put2;
		Put put3;
		Put put4;
		
		Put update1;
		Put update2;
		Put update3;
		
		Delete delete;
		
		switch(updateIndex) {
			/*
			 * Single selection
			 * Combined selection
			 * Sum aggregation
			 * Sum aggregation combined with selection
			 * Count aggregation
			 * Count aggregation combined with selection
			 */
			case 0: case 1: case 2: case 3: case 4: case 5:
				rowKey1 = "k0001";
				rowKey2 = "k0002";
				rowKey3 = "k0003";
				rowKey4 = "k0004";
				
				// Insert
				put1 = new Put(Bytes.toBytes(rowKey1));
				put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("30"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey1), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put1);
				put2 = new Put(Bytes.toBytes(rowKey2));
				put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0002"));
				put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("70"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey2), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put2);
				put3 = new Put(Bytes.toBytes(rowKey3));
				put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("50"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey3), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put3);
				put4 = new Put(Bytes.toBytes(rowKey4));
				put4.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				put4.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("90"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey4), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put4);
				
				// Update
				update1 = new Put(Bytes.toBytes(rowKey1));
				update1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0003"));
				update1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("5"));
				baseTable.put(update1);
				update2 = new Put(Bytes.toBytes(rowKey2));
				update2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				update2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("40"));
				baseTable.put(update2);
				update3 = new Put(Bytes.toBytes(rowKey3));
				update3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				update3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("10"));
				baseTable.put(update3);
				
				// Delete
				delete = new Delete(Bytes.toBytes(rowKey4));
				baseTable.delete(delete);
				break;
			/*
			 * Min aggregation
			 * Min aggregation combined with selection
			 */
			case 6: case 7: 
				rowKey1 = "k0001";
				rowKey2 = "k0002";
				rowKey3 = "k0003";
				rowKey4 = "k0004";
				
				// Insert
				put1 = new Put(Bytes.toBytes(rowKey1));
				put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("30"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey1), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put1);
				put2 = new Put(Bytes.toBytes(rowKey2));
				put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0002"));
				put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("70"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey2), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put2);
				put3 = new Put(Bytes.toBytes(rowKey3));
				put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("50"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey3), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put3);
				put4 = new Put(Bytes.toBytes(rowKey4));
				put4.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
				put4.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("90"));
				baseTable.checkAndPut(Bytes.toBytes(rowKey4), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put4);
				
				// Update
				update1 = new Put(Bytes.toBytes(rowKey3));
				update1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0002"));
				update1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("60"));
				baseTable.put(update1);
				
				// Delete
				delete = new Delete(Bytes.toBytes(rowKey1));
				baseTable.delete(delete);
				break;
				/*
				 * Max aggregation
				 * Max aggregation combined with selection
				 */
				case 8: case 9: 
					rowKey1 = "k0001";
					rowKey2 = "k0002";
					rowKey3 = "k0003";
					rowKey4 = "k0004";
					
					// Insert
					put1 = new Put(Bytes.toBytes(rowKey1));
					put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
					put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("30"));
					baseTable.checkAndPut(Bytes.toBytes(rowKey1), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put1);
					put2 = new Put(Bytes.toBytes(rowKey2));
					put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0002"));
					put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("70"));
					baseTable.checkAndPut(Bytes.toBytes(rowKey2), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put2);
					put3 = new Put(Bytes.toBytes(rowKey3));
					put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
					put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("50"));
					baseTable.checkAndPut(Bytes.toBytes(rowKey3), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put3);
					put4 = new Put(Bytes.toBytes(rowKey4));
					put4.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0001"));
					put4.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("90"));
					baseTable.checkAndPut(Bytes.toBytes(rowKey4), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put4);
					
					// Update
					update1 = new Put(Bytes.toBytes(rowKey3));
					update1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey1"), Bytes.toBytes("x0002"));
					update1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal1"), Bytes.toBytes("80"));
					baseTable.put(update1);
					
					// Delete
					delete = new Delete(Bytes.toBytes(rowKey4));
					baseTable.delete(delete);
					break;
		}
	}
}
