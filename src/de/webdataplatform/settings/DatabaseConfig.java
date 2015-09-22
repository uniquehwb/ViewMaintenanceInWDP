package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import de.webdataplatform.log.Log;
import de.webdataplatform.sql.BaseTableDefinition;
import de.webdataplatform.sql.SqlClient;
import de.webdataplatform.sql.Table;

public class DatabaseConfig {


	
	
	public static List<TableDefinition> TABLEDEFINITIONS;
	public static List<String> PKPREFIX = getBaseTablePKPrefix();
	
	// Initialization of base tables, including column information of the base table.
	// Actually base tables should be predefined with all necessary information available.
	public static List<BaseTableDefinition> getBaseTableDefinition() {
		List<BaseTableDefinition> baseTableDefinition = new ArrayList<BaseTableDefinition>();
		
		Map<String, String> columns1 = new HashMap<String, String>();
		columns1.put("colfam1:colAggKey1", "x");
		columns1.put("colfam1:colAggVal1", "");
		BaseTableDefinition bt1 = new BaseTableDefinition("bt1", "k", columns1);
		baseTableDefinition.add(bt1);
		
		Map<String, String> columns2 = new HashMap<String, String>();
		columns2.put("colfam1:colAggKey2", "x");
		columns2.put("colfam1:colAggVal2", "y");
		BaseTableDefinition bt2 = new BaseTableDefinition("bt2", "l", columns2);
		baseTableDefinition.add(bt2);
		
		return baseTableDefinition;
	}
	
	public static List<String> getBaseTablePKPrefix() {
		List<String> prefix = new ArrayList<String>();
		for (BaseTableDefinition btDef: getBaseTableDefinition()) {
			prefix.add(btDef.getRowkeyPrefix());
		}
		return prefix;
	}
	
	public static BaseTableDefinition getBaseTableDefinitionByName(String name) {
		for (BaseTableDefinition btDef: getBaseTableDefinition()) {
    		if (name.equals(btDef.getName())) {
    			return btDef;
    		}
    	}
		return null;
	}
	
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
		    	BaseTableDefinition btDef = getBaseTableDefinitionByName(table.getName());
		    	for (String column: btDef.getColumns().keySet()) {
		    		String colName = column.split(":")[1];
				    String colFamily = column.split(":")[0];
				    String colPrefix = btDef.getColumns().get(column);
				    String colStartRange = "1";
				    String colEndRange = "101";
				    
				    ColumnDefinition colDef = new ColumnDefinition(colName, colFamily, colPrefix, Long.parseLong(colStartRange), Long.parseLong(colEndRange));
				    colDefs.add(colDef);
		    	}
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
