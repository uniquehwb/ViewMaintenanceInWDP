package de.webdataplatform.sql;

import java.util.List;

import javax.xml.transform.TransformerException;

import net.sf.jsqlparser.JSQLParserException;

public class SqlClient {
	private String queryString;
	
	public SqlClient(String queryString) {
		this.queryString = queryString;
	}
	
	public static void main(String[] args) throws JSQLParserException, TransformerException {
		
		String queryString = "SELECT c1, SUM (c2) FROM t1 WHERE c2 < 10 GROUP BY (c1)";
//		String queryString = "SELECT t1.c1, t2.c2, SUM (t1.c2) "
//						   + "FROM t1 INNER JOIN t2 ON t1.c1 = t2.c1 "
//						   + "WHERE t1.c2 > 10 AND t2.c3 = 20 "
//						   + "GROUP BY t1.c1 ";
//		String queryString = "SELECT c1, c4, SUM(c2) FROM MY_TABLE1, MY_TABLE2, (SELECT c1 FROM MY_TABLE3) LEFT OUTER JOIN MY_TABLE4 "+
//							 "WHERE ID = (SELECT MAX(ID) FROM MY_TABLE5) AND ID2 IN (SELECT * FROM MY_TABLE6) "+
//							 "GROUP BY c1";
		
		// Parse the query string to views
		JSqlParser parser = new JSqlParser(queryString);
		// From views to view chain
		List<Table> output = parser.parse();
		// Print out view chain in string
		LogicPlan.toString(output);
		
		DatabaseXMLGenerator dbXMLGenerator = new DatabaseXMLGenerator(output);
		EvaluationXMLGenerator evaXMLGenerator = new EvaluationXMLGenerator(output);
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}
	
	public void generateDataset() throws JSQLParserException, TransformerException {
		// Parse the query string to views
		JSqlParser parser = new JSqlParser(queryString);
		// From views to view chain
		List<Table> output = parser.parse();
		// Print out view chain in string
		LogicPlan.toString(output);
		
		DatabaseXMLGenerator dbXMLGenerator = new DatabaseXMLGenerator(output);
		EvaluationXMLGenerator evaXMLGenerator = new EvaluationXMLGenerator(output);
	}
}
