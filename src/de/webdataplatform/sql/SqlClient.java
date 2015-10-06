package de.webdataplatform.sql;

import java.util.List;

import net.sf.jsqlparser.JSQLParserException;

public class SqlClient {
	
	private String queryString;
	
	public static int queryIndex = 16;
	
	public SqlClient() {
		
		switch(queryIndex) {
			// Single selection
			case 0: 
				queryString = "SELECT colAggKey1 FROM bt1 WHERE colAggVal1 > 10";
				break;
			// Combined selection
			case 1: 
				queryString = "SELECT colAggKey1 FROM bt1 WHERE colAggVal1 < 60 AND colAggVal1 > 10";
				break;
			// Sum aggregation
			case 2:
				queryString = "SELECT SUM (colAggVal1) FROM bt1 GROUP BY colAggKey1";
				break;
			// Sum aggregation combined with selection
			case 3:
				queryString = "SELECT colAggKey1, SUM (colAggVal1) FROM bt1 WHERE colAggVal1 > 10 GROUP BY colAggKey1";
				break;
			// Count aggregation
			case 4:
				queryString = "SELECT COUNT (colAggVal1) FROM bt1 GROUP BY colAggKey1";
				break;
			// Count aggregation combined with selection
			case 5:
				queryString = "SELECT COUNT (colAggVal1) FROM bt1 WHERE colAggVal1 > 10 GROUP BY colAggKey1";
				break;
			// Min aggregation
			case 6:
				queryString = "SELECT MIN (colAggVal1) FROM bt1 GROUP BY colAggKey1";
				break;
			// Min aggregation combined with selection
			case 7:
				queryString = "SELECT MIN (colAggVal1) FROM bt1 WHERE colAggVal1 < 80 GROUP BY colAggKey1";
				break;
			// Max aggregation
			case 8:
				queryString = "SELECT MAX (colAggVal1) FROM bt1 GROUP BY colAggKey1";
				break;
			// Max aggregation combined with selection
			case 9:
				queryString = "SELECT MAX (colAggVal1) FROM bt1 WHERE colAggVal1 < 80 GROUP BY colAggKey1";
				break;
			// Join
			case 10:
				queryString = "SELECT * FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2";
				break;
			// Join and selection
			case 11:
				queryString = "SELECT bt1.colAggKey1 "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 < 80 ";
				break;
			// Join and sum
			case 12:
				queryString = "SELECT bt1.colAggKey1, SUM (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Join and count
			case 13:
				queryString = "SELECT bt1.colAggKey1, COUNT (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Join and min
			case 14:
				queryString = "SELECT bt1.colAggKey1, MIN (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Join and max
			case 15:
				queryString = "SELECT bt1.colAggKey1, MAX (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Selection, join and sum
			case 16:
				queryString = "SELECT bt1.colAggKey1, SUM (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 < 80 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Selection, join and count
			case 17:
				queryString = "SELECT bt1.colAggKey1, COUNT (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 < 80 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Selection, join and min
			case 18:
				queryString = "SELECT bt1.colAggKey1, MIN (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 < 80 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Selection, join and max
			case 19:
				queryString = "SELECT bt1.colAggKey1, MAX (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 < 80 "+
					      "GROUP BY colAggVal2 ";
				break;
			default:
				queryString = "Invalid query";
            	break;
		}
	}
	
	// For testing
	public static void main(String[] args){
		String queryString;
		
		switch(queryIndex) {
			// Single selection
			case 0: 
				queryString = "SELECT colAggKey1 FROM bt1 WHERE colAggVal1 > 10";
				break;
			// Combined selection
			case 1: 
				queryString = "SELECT colAggKey1 FROM bt1 WHERE colAggVal1 < 60 AND colAggVal1 > 10";
				break;
			// Sum aggregation
			case 2:
				queryString = "SELECT SUM (colAggVal1) FROM bt1 GROUP BY colAggKey1";
				break;
			// Sum aggregation combined with selection
			case 3:
				queryString = "SELECT colAggKey1, SUM (colAggVal1) FROM bt1 WHERE colAggVal1 > 10 GROUP BY colAggKey1";
				break;
			// Count aggregation
			case 4:
				queryString = "SELECT COUNT (colAggVal1) FROM bt1 GROUP BY colAggKey1";
				break;
			// Count aggregation combined with selection
			case 5:
				queryString = "SELECT colAggKey1, COUNT (colAggVal1) FROM bt1 WHERE colAggVal1 > 10 GROUP BY colAggKey1";
				break;
			// Min aggregation
			case 6:
				queryString = "SELECT MIN (colAggVal1) FROM bt1 GROUP BY colAggKey1";
				break;
			// One join
			case 7:
				queryString = "SELECT colAggKey1 FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2";
				break;
			// Join and selection
			case 8:
				queryString = "SELECT bt1.colAggKey1 "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 > 10 AND bt2.colAggVal2 < 80 ";
				break;
			// Join and sum
			case 9:
				queryString = "SELECT bt1.colAggKey1, SUM (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Join and count
			case 10:
				queryString = "SELECT bt1.colAggKey1, COUNT (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Selection, join and sum
			case 11:
				queryString = "SELECT bt1.colAggKey1, SUM (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 < 90 "+
					      "GROUP BY colAggVal2 ";
				break;
			// Selection, join and count
			case 12:
				queryString = "SELECT bt1.colAggKey1, COUNT (colAggVal1) "+
					      "FROM bt1 INNER JOIN bt2 ON bt1.colAggKey1 = bt2.colAggKey2 "+
					      "WHERE bt1.colAggVal1 < 90 "+
					      "GROUP BY colAggVal2 ";
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
