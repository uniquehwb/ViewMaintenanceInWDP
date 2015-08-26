package de.webdataplatform.sql;

import java.util.List;

import net.sf.jsqlparser.JSQLParserException;

public class SqlClient {
	private String queryString;
	
	public SqlClient() {
		
		int queryIndex = 2;
		
		switch(queryIndex) {
			case 0: 
				queryString = "SELECT colAggKey FROM bt1 WHERE colAggVal < 20";
				break;
			case 1:
				queryString = "SELECT SUM (colAggVal) FROM bt1 GROUP BY (colAggKey)";
				break;
			case 2:
				queryString = "SELECT colAggKey, SUM (colAggVal) FROM bt1 WHERE colAggVal > 90 GROUP BY (colAggKey)";
				break;
			case 3:
				queryString = "SELECT colAggKey FROM bt1 INNER JOIN bt2";
				break;
			case 4:
				queryString = "SELECT bt1.colAggKey, SUM (t1.colAggVal) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey = bt2.colAggKey "+
					      "WHERE bt1.colAggVal > 10 AND bt2.colAggVal = 20 "+
					      "GROUP BY bt1.colAggKey ";
				break;
			default:
				queryString = "Invalid query";
            	break;
		}
	}
	
	// For testing
	public static void main(String[] args){
		int queryIndex = 3;
		String queryString;
		switch(queryIndex) {
			case 0: 
				queryString = "SELECT colAggKey FROM bt1 WHERE colAggVal < 20";
				break;
			case 1:
				queryString = "SELECT SUM (colAggVal) FROM bt1 GROUP BY (colAggKey)";
				break;
			case 2:
				queryString = "SELECT colAggKey, SUM (colAggVal) FROM bt1 WHERE colAggVal > 90 GROUP BY (colAggKey)";
				break;
			case 3:
				queryString = "SELECT colAggKey FROM bt1 INNER JOIN bt2 on bt1.colAggKey = bt2.colAggKey";
				break;
			case 4:
				queryString = "SELECT bt1.colAggKey, SUM (t1.colAggVal) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey = bt2.colAggKey "+
					      "WHERE bt1.colAggVal > 10 AND bt2.colAggVal = 20 "+
					      "GROUP BY bt1.colAggKey ";
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
