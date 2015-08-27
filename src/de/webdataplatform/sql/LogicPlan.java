package de.webdataplatform.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LogicPlan {
	// store all operation objects parsed from query
	private List<Table> tableList;
	// store operation list based on different base table
	private Map<String, List<Table>> tableListMap; 
	// output is a list of tables and views with relationships between
	private List<Table> output = new ArrayList<Table>();
	
	public LogicPlan(List<Table> tableList, Map<String, List<Table>> tableListMap) {
		this.tableList = tableList;
		this.tableListMap = tableListMap;
	}
	
	public List<Table> generatePlan() {
		// there is no join
		if (tableListMap.isEmpty()) {
			output = generatePartialPlan(tableList);
		} else {
			// store basetable names of the join
			List<Table> baseTables = new ArrayList<Table>();
			Iterator it = tableListMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				List<Table> list = (List<Table>) pair.getValue();
				List<Table> outputList = generatePartialPlan(list);
				output.addAll(outputList);
				baseTables.add(outputList.get(outputList.size()-1));
			}
			// if there is join, the first object of tableList is always join view
			tableList.get(0).setBaseTables(baseTables);
			output.addAll(generatePartialPlan(tableList));
		}
		return output;
	}
	
	public List<Table> generatePartialPlan(List<Table> list) {
		for (int i = 1; i < list.size(); i++) {
			List<Table> basetables = new ArrayList<Table>();
			basetables.add(list.get(i-1));
			list.get(i).setBaseTables(basetables);
			list.get(i).setControlTable(list.get(0));
		}
		return list;
	}
	
	public static void toString(List<Table> tables) {
		String plainString = "";
		for (Table table: tables) {
			plainString += "Table name: " + table.getName();
			plainString += "\n";
			plainString += "Table PK prefix: " + table.getPKPrefix();
			plainString += "\n";
			plainString += "Table type: " + table.getType();
			if (table.getBaseTables() != null) {
				plainString += "\n";
				plainString += "Base tables: ";
				for (Table basetable: table.getBaseTables()) {
					plainString += basetable.getName() + " ";
				}
			}
			plainString += "\n\n";
		}
		System.out.println(plainString);
	}
}
