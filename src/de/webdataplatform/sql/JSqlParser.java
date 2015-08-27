package de.webdataplatform.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableName;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.util.TablesNamesFinder;


public class JSqlParser {
	private String sqlString;
	private Statement statement;
	// store all operation objects parsed from query
	private List<Table> tableList;
	// store operation list based on different base table
	private Map<String, List<Table>> tableListMap;
	
	private boolean containJoin = false;
	// count the number of view to avoid duplicated name
	private int basetableCount = 1;
	private int selectionViewCount = 1;
	private int projectionViewCount = 1;
	private int deltaViewCount = 1;
	private int aggregationViewCount = 1;
	private int joinViewCount = 1;
	
	private AggregationsFinder aggregationsFinder;
	private TablesNamesFinder tablesNamesFinder;
	
	private char startRowKeyPrefix = 'j';
	private char startAggKeyPrefix = 'x';
	
	
	public JSqlParser(String sqlString) {
		this.sqlString = sqlString;
		aggregationsFinder = new AggregationsFinder();
		tablesNamesFinder = new TablesNamesFinder();
		tableList = new ArrayList<Table>();
		tableListMap = new HashMap<String, List<Table>>();
	}
	
	public List<Table> parse() throws JSQLParserException{
		List<Table> output = new ArrayList<Table>();
		statement = CCJSqlParserUtil.parse(sqlString);
		if (statement instanceof Select) {
			SelectBody select = ((Select) statement).getSelectBody();
			// Single
			if (select instanceof PlainSelect) {
				PlainSelect pselect = (PlainSelect) select;
				getAllKindsOfTables(pselect);
				
				LogicPlan logicPlan = new LogicPlan(tableList, tableListMap);
				output = logicPlan.generatePlan();
				
			}
		}
		return output;
	}
	
	public void getAllKindsOfTables(PlainSelect pselect) {
		// Get join
		if (pselect.getJoins() != null) {
			// In use case, there is at most one join.
			Join join = (Join) pselect.getJoins().get(0);
			Table reverseJoinView = new ReverseJoinView("join" + joinViewCount);
			String prefix = String.valueOf((char) (startAggKeyPrefix));
			reverseJoinView.setPKPrefix(prefix);
			joinViewCount++;
			reverseJoinView.setExpression(join.toString());
			if (join.getOnExpression() instanceof BinaryExpression) {
				BinaryExpression binaryExpression = (BinaryExpression) join.getOnExpression();
				if (binaryExpression.getLeftExpression() instanceof Column) {
					Column column = (Column) binaryExpression.getLeftExpression();
					String aggregationKey = column.getColumnName();
//						keyWords = keyWords + aggregationKey;
				}
			}
			tableList.add(reverseJoinView);
			
			List<String> tablesNamesList = tablesNamesFinder.getTableList((Select) statement);
			if (tablesNamesList != null) {
				for (int m = 0; m < tablesNamesList.size(); m++) {
					String name = tablesNamesList.get(m);
					List<Table> list;
					list = new ArrayList<Table>();
					// Add base table
					Table basetable = new BaseTable(name);
					String btPrefix = String.valueOf((char) (startRowKeyPrefix + basetableCount));
					basetable.setPKPrefix(btPrefix);
					list.add(basetable);
					basetableCount++;
					// Add delta view preparing for the join
					Table deltaView = new DeltaView("delta" + deltaViewCount);
					String deltaPrefix = String.valueOf((char) (startRowKeyPrefix + deltaViewCount));
					deltaView.setPKPrefix(deltaPrefix);
					list.add(deltaView);
					deltaViewCount++;
					tableListMap.put(name, list);
				}
			}
			containJoin = true;
		} else {
			System.out.println("no joins \n");
			List<String> tablesNamesList = tablesNamesFinder.getTableList((Select) statement);
			if (tablesNamesList != null) {
				// There should be only one base table without join.
				Table basetable = new BaseTable(tablesNamesList.get(0));
				String btPrefix = String.valueOf((char) (startRowKeyPrefix + basetableCount));
				basetable.setPKPrefix(btPrefix);
				tableList.add(basetable);
				basetableCount++;
				Table deltaView = new DeltaView("delta" + deltaViewCount);
				String deltaPrefix = String.valueOf((char) (startRowKeyPrefix + deltaViewCount));
				deltaView.setPKPrefix(deltaPrefix);
				tableList.add(deltaView);
				deltaViewCount++;
			}
			containJoin = false;
		}
			
		// Get selection
		// Handle selection with join and without join separately
		if (pselect.getWhere() != null) {
			Expression e = (Expression) pselect.getWhere();
			try {
				while (e instanceof AndExpression) {
					AndExpression andExpression = (AndExpression) e;
					Table selectionView = new SelectionView("selection" + selectionViewCount);
					selectionViewCount++;
					Expression rightExpression = andExpression.getRightExpression();
					String expression = rightExpression.toString();
					selectionView.setExpression(expression);
					if (expression.contains(">")) {
						selectionView.setFirstAttr(">");
					    selectionView.setSecondAttr(expression.split(">")[1].trim());
				    } else if (expression.contains("=")) {
				    	selectionView.setFirstAttr("=");
					    selectionView.setSecondAttr(expression.split("=")[1].trim());
				    } else {
				    	selectionView.setFirstAttr("<");
					    selectionView.setSecondAttr(expression.split("<")[1].trim());
				    }
					
					if (containJoin) {
						// If there are joins, put selections to corresponding base table.
						if (rightExpression instanceof BinaryExpression) {
							BinaryExpression binaryExpression = (BinaryExpression) rightExpression;
							if (binaryExpression.getLeftExpression() instanceof Column) {
								Column column = (Column) binaryExpression.getLeftExpression();
								String baseTable = column.getTable().toString();
								List<Table> list;
								list = tableListMap.get(baseTable);
								// In each chain, prefix of primary key should be same as base table
								selectionView.setPKPrefix(list.get(0).getPKPrefix());
								list.add(selectionView);
								tableListMap.put(baseTable, list);
							}
						}
					} else {
						selectionView.setPKPrefix(tableList.get(0).getPKPrefix());
						tableList.add(selectionView);
					}
					// Iterator
					e = andExpression.getLeftExpression();
				}
			} finally {
				// Last selection without "and"
				Table selectionView = new SelectionView("selection" + selectionViewCount);
				selectionViewCount++;
				String expression = e.toString();
				selectionView.setExpression(expression);
				if (expression.contains(">")) {
					selectionView.setFirstAttr(">");
				    selectionView.setSecondAttr(expression.split(">")[1].trim());
			    } else if (expression.contains("=")) {
			    	selectionView.setFirstAttr("=");
				    selectionView.setSecondAttr(expression.split("=")[1].trim());
			    } else {
			    	selectionView.setFirstAttr("<");
				    selectionView.setSecondAttr(expression.split("<")[1].trim());
			    }
				if (containJoin) {
					if (e instanceof BinaryExpression) {
						BinaryExpression binaryExpression = (BinaryExpression) e;
						if (binaryExpression.getLeftExpression() instanceof Column) {
							Column column = (Column) binaryExpression.getLeftExpression();
							String baseTable = column.getTable().toString();
							List<Table> list;
							list = tableListMap.get(baseTable);
							// In each chain, prefix of primary key should be same as base table
							selectionView.setPKPrefix(list.get(0).getPKPrefix());
							list.add(selectionView);
							tableListMap.put(baseTable, list);
							
						}
					}
				} else {
					selectionView.setPKPrefix(tableList.get(0).getPKPrefix());
					tableList.add(selectionView);
				}
			}
		} else {
			System.out.println("no where");
		}
		
		// Get aggregation
		if (aggregationsFinder.getAggregationList(pselect) != null) {
			for (Function fun: aggregationsFinder.getAggregationList(pselect)) {
				String aggregationType = fun.getName().toLowerCase();
				Table aggregationView = new AggregationView(aggregationType + aggregationViewCount);
				aggregationView.setType(aggregationType);
				String aggregationPrefix = String.valueOf((char) (startAggKeyPrefix));
				aggregationView.setPKPrefix(aggregationPrefix);
				aggregationViewCount++;
				aggregationView.setExpression(fun.toString());
				// group by key
				String groupByKey = pselect.getGroupByColumnReferences().get(0).toString();
				aggregationView.setFirstAttr(groupByKey);
				// aggregation key
				String aggKey = fun.getParameters().getExpressions().get(0).toString();
				aggregationView.setSecondAttr(aggKey);
				tableList.add(aggregationView);
//				System.out.println(aggregationView.getFirstAttr() + " " + aggregationView.getSecondAttr());
			}
		}
				
		// Get projection
//		if (pselect.getSelectItems() != null) {
//			Table projectionView = new ProjectionView("projection" + viewCount);
//			viewCount++;
//			projectionView.setExpression(pselect.getSelectItems().toString());
//			tableList.add(projectionView);
//		}
		
	}
}
