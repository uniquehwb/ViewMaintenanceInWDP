package de.webdataplatform.sql;

import java.util.List;

import net.sf.jsqlparser.JSQLParserException;

public class SqlClient {
	private String queryString;
	
	public SqlClient() {
		
		int queryIndex = 8;
		
		switch(queryIndex) {
			// Single selection
			case 0: 
				queryString = "SELECT colAggKey FROM bt1 WHERE colAggVal < 60";
				break;
			// Combined selection
			case 1: 
				queryString = "SELECT colAggKey FROM bt1 WHERE colAggVal < 60 AND colAggVal > 40";
				break;
			// Sum aggregation
			case 2:
				queryString = "SELECT SUM (colAggVal) FROM bt1 GROUP BY colAggKey";
				break;
			// Sum aggregation combined with selection
			case 3:
				queryString = "SELECT colAggKey, SUM (colAggVal) FROM bt1 WHERE colAggVal > 90 GROUP BY colAggKey";
				break;
			// Count aggregation
			case 4:
				queryString = "SELECT COUNT (colAggVal) FROM bt1 GROUP BY colAggKey";
				break;
			// Count aggregation combined with selection
			case 5:
				queryString = "SELECT colAggKey, COUNT (colAggVal) FROM bt1 WHERE colAggVal > 80 GROUP BY colAggKey";
				break;
			// One join
			case 6:
				queryString = "SELECT colAggKey FROM bt1 INNER JOIN bt2";
				break;
			// Two join
			case 7:
				queryString = "SELECT colAggKey FROM bt1 INNER JOIN bt2 INNER JOIN bt3";
				break;
			case 8:
				queryString = "SELECT bt1.colAggKey, SUM (colAggVal) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey = bt2.colAggKey "+
//					      "WHERE bt1.colAggVal > 50 AND bt2.colAggVal < 50 "+
					      "GROUP BY colAggKey ";
				break;
			default:
				queryString = "Invalid query";
            	break;
		}
	}
	
	// For testing
	public static void main(String[] args){
		int queryIndex = 8;
		String queryString;
		
		switch(queryIndex) {
			// Single selection
			case 0: 
				queryString = "SELECT colAggKey FROM bt1 WHERE colAggVal < 60";
				break;
			// Combined selection
			case 1: 
				queryString = "SELECT colAggKey FROM bt1 WHERE colAggVal < 60 AND colAggVal > 40";
				break;
			// Sum aggregation
			case 2:
				queryString = "SELECT SUM (colAggVal) FROM bt1 GROUP BY colAggKey";
				break;
			// Sum aggregation combined with selection
			case 3:
				queryString = "SELECT colAggKey, SUM (colAggVal) FROM bt1 WHERE colAggVal > 90 GROUP BY colAggKey";
				break;
			// Count aggregation
			case 4:
				queryString = "SELECT COUNT (colAggVal) FROM bt1 GROUP BY colAggKey";
				break;
			// Count aggregation combined with selection
			case 5:
				queryString = "SELECT colAggKey, COUNT (colAggVal) FROM bt1 WHERE colAggVal > 90 GROUP BY colAggKey";
				break;
			// Single join
			case 6:
				queryString = "SELECT colAggKey FROM bt1 INNER JOIN bt2";
				break;
			// Double join
			case 7:
				queryString = "SELECT colAggKey FROM bt1 INNER JOIN bt2 INNER JOIN bt3";
				break;
			case 8:
				queryString = "SELECT bt1.colAggKey, SUM (colAggVal) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey = bt2.colAggKey "+
//					      "WHERE bt1.colAggVal > 80 AND bt2.colAggVal < 20 "+
					      "GROUP BY colAggKey ";
				break;
			default:
				queryString = "Invalid query";
	        	break;
		}
		try {
			// Parse the query string to views
			JSqlParser parser = new JSqlParser(queryString);
			// From views to view chain
			List<Table> output = parser.parse();
			// Print out view chain in string
			LogicPlan.toString(output);
		} catch (JSQLParserException je) {
		}
	}
	
	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}
	
	public List<Table> generateDataset(){
		try {
			// Parse the query string to views
			JSqlParser parser = new JSqlParser(queryString);
			// From views to view chain
			List<Table> output = parser.parse();
			// Print out view chain in string
			LogicPlan.toString(output);
			return output;
		} catch (JSQLParserException je) {
			return null;
		}
	}
}
