package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import de.webdataplatform.log.Log;
import de.webdataplatform.sql.SqlClient;
import de.webdataplatform.sql.Table;

public class DatabaseConfig {


	
	
	public static List<TableDefinition> TABLEDEFINITIONS;
	
	public static TableDefinition getTableDefinition(String name){
		
		for (TableDefinition tableDefinition : TABLEDEFINITIONS) {
			
			
			if(tableDefinition.getName().equals(name))return tableDefinition;
			
		}
		return null;
	}
	
	public static void load(Log log) {
		log.info(DatabaseConfig.class, "Loading database config:");
		
		SqlClient sqlClient = new SqlClient();
		List<Table> tables = sqlClient.generateDataset();
		
	    DatabaseConfig.TABLEDEFINITIONS = new ArrayList<TableDefinition>();
	    
	    for (int i = 0; i < tables.size(); i++) {
		
	    	Table table = tables.get(i);
		    
		    String keyPrefix = table.getPKPrefix();
		    String keyStartRange = "1";
		    String keyEndRange = "1001";
		    
		    KeyDefinition keyDef = new KeyDefinition(keyPrefix, Long.parseLong(keyStartRange), Long.parseLong(keyEndRange));
		    
		    
		    			    
		    List<ColumnDefinition> colDefs = new ArrayList<ColumnDefinition>();
		    
		    if ("basetable".equals(table.getType())) {
		    	String colName1 = "colAggKey1";
			    String colFamily1 = "colfam1";
			    String colPrefix1 = "x";
			    String colStartRange1 = "1";
			    String colEndRange1 = "1001";
			    
			    String colName2 = "colAggVal1";
			    String colFamily2 = "colfam1";
			    String colPrefix2 = "";
			    String colStartRange2 = "1";
			    String colEndRange2 = "101";
			    
			    ColumnDefinition colDef1 = new ColumnDefinition(colName1, colFamily1, colPrefix1, Long.parseLong(colStartRange1), Long.parseLong(colEndRange1));
			    ColumnDefinition colDef2 = new ColumnDefinition(colName2, colFamily2, colPrefix2, Long.parseLong(colStartRange2), Long.parseLong(colEndRange2));
			    
			    colDefs.add(colDef1);
			    colDefs.add(colDef2);
		    }
		    
		    if (table.getBaseTables() != null) {
		    	if ("reversejoin".equals(table.getType())) {
					for (int j = 0; j < table.getBaseTables().size(); j++) {
						String colName = "colAggVal" + (j+1);
					    String colFamily = table.getBaseTables().get(j).getType() + (j+1) + "fam1";
					    String colPrefix = "";
					    String colStartRange = "1";
					    String colEndRange = "101";
					    
					    ColumnDefinition colDef = new ColumnDefinition(colName, colFamily, colPrefix, Long.parseLong(colStartRange), Long.parseLong(colEndRange));
					    colDefs.add(colDef);
					}
					// Add colfam1 to reversejoin view for recording zfinish_marker
					String colName = "zfinish_marker";
				    String colFamily = "colfam1";
				    String colPrefix = "";
				    String colStartRange = "1";
				    String colEndRange = "101";
				    
				    ColumnDefinition colDef = new ColumnDefinition(colName, colFamily, colPrefix, Long.parseLong(colStartRange), Long.parseLong(colEndRange));
				    colDefs.add(colDef);
				    
				    // Add join family
					String colName1 = "";
				    String colFamily1 = "joinfam1";
				    String colPrefix1 = "";
				    String colStartRange1 = "1";
				    String colEndRange1 = "101";
				    
				    ColumnDefinition colDef1 = new ColumnDefinition(colName1, colFamily1, colPrefix1, Long.parseLong(colStartRange1), Long.parseLong(colEndRange1));
				    colDefs.add(colDef1);
				    
				    // Add count family
					String colName2 = "";
				    String colFamily2 = "countfam1";
				    String colPrefix2 = "";
				    String colStartRange2 = "1";
				    String colEndRange2 = "101";
				    
				    ColumnDefinition colDef2 = new ColumnDefinition(colName2, colFamily2, colPrefix2, Long.parseLong(colStartRange2), Long.parseLong(colEndRange2));
				    colDefs.add(colDef2);
		    	} else {
		    		String colName = "colAggVal1";
				    String colFamily = "colfam1";
				    String colPrefix = "";
				    String colStartRange = "1";
				    String colEndRange = "101";
				    
				    ColumnDefinition colDef = new ColumnDefinition(colName, colFamily, colPrefix, Long.parseLong(colStartRange), Long.parseLong(colEndRange));
				    colDefs.add(colDef);
		    	}
		    }
		    
		    TableDefinition tableDefinition = new TableDefinition(table.getName(), keyDef, colDefs);
		    
		    log.info(DatabaseConfig.class, tableDefinition.toString());
		    
		    DatabaseConfig.TABLEDEFINITIONS.add(tableDefinition);
		    
		    

		}

		    

		log.info(DatabaseConfig.class, "--------------------------------------------------------------");
  



	}
	
	public static void main(String[] args){
//		 load();
	}
	

}
