package de.webdataplatform.settings;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import de.webdataplatform.log.Log;
import de.webdataplatform.sql.SqlClient;
import de.webdataplatform.sql.Table;

public class EvaluationConfig {


	
	public static Long UPDATEPROCESSTIMEOUT;
	
	public static Long REGIONSERVER_UPDATESTIMEOUT;
	public static Long REGIONSERVER_OVERALLTIMEOUT;
	public static Long REGIONSERVER_READWALDELAY;
	
	public static Long CLIENT_FILLTABLESTIMEOUT;
	public static Boolean CLIENT_CREATECONTROLTABLES;
	public static Boolean CLIENT_RECREATETABLES;
	// If true, massive random updates are issued to base tables; if false, specific updates are
	// issued to base tables to check the correctness of results.
	public static Boolean CLIENT_PERFORMANCE_TEST;
	
	public static Long VIEWMANAGER_KILLDELAY;
	
	public static List<EvaluationSet> EVALUATIONSETS;
	
	
	
	
	
	public static void load(Log log) {
	
		
		
		
		try
		{
			
			log.info(EvaluationConfig.class, "Loading evaluation config:");
			
			XMLConfiguration config = new XMLConfiguration();
			config.setDelimiterParsingDisabled(true);
			config.load("VMEvaluationConfig.xml");
			
			SqlClient sqlClient = new SqlClient();
			List<Table> tables = sqlClient.generateDataset();
			
			EvaluationConfig.UPDATEPROCESSTIMEOUT = Long.parseLong(config.getString("updateProcessTimeout"));
			
		    EvaluationConfig.REGIONSERVER_UPDATESTIMEOUT = Long.parseLong(config.getString("regionServer.updatesTimeout"));
		    EvaluationConfig.REGIONSERVER_OVERALLTIMEOUT = Long.parseLong(config.getString("regionServer.overallTimeout"));
		    EvaluationConfig.REGIONSERVER_READWALDELAY = Long.parseLong(config.getString("regionServer.readWALDelay"));
		    
		    EvaluationConfig.CLIENT_FILLTABLESTIMEOUT = Long.parseLong(config.getString("client.fillTablesTimeout"));
		    EvaluationConfig.CLIENT_CREATECONTROLTABLES = Boolean.parseBoolean(config.getString("client.createControlTables"));	
		    EvaluationConfig.CLIENT_RECREATETABLES = Boolean.parseBoolean(config.getString("client.recreateTables"));	
		    EvaluationConfig.CLIENT_PERFORMANCE_TEST = Boolean.parseBoolean(config.getString("client.performanceTest"));
		    
		    EvaluationConfig.VIEWMANAGER_KILLDELAY = Long.parseLong(config.getString("viewManager.killDelay"));	
		    
		    
		    log.info(SystemConfig.class, "EvaluationConfig.UPDATEPROCESSTIMEOUT:"+EvaluationConfig.UPDATEPROCESSTIMEOUT);

		    log.info(SystemConfig.class, "EvaluationConfig.REGIONSERVER_UPDATESTIMEOUT:"+EvaluationConfig.REGIONSERVER_UPDATESTIMEOUT);
		    log.info(SystemConfig.class, "EvaluationConfig.REGIONSERVER_OVERALLTIMEOUT:"+EvaluationConfig.REGIONSERVER_OVERALLTIMEOUT);
		    log.info(SystemConfig.class, "EvaluationConfig.REGIONSERVER_READWALDELAY:"+EvaluationConfig.REGIONSERVER_READWALDELAY);

		    log.info(SystemConfig.class, "EvaluationConfig.CLIENT_FILLTABLESTIMEOUT:"+EvaluationConfig.CLIENT_FILLTABLESTIMEOUT);
		    log.info(SystemConfig.class, "EvaluationConfig.CLIENT_CREATECONTROLTABLES:"+EvaluationConfig.CLIENT_CREATECONTROLTABLES);
		    log.info(SystemConfig.class, "EvaluationConfig.CLIENT_RECREATETABLES:"+EvaluationConfig.CLIENT_RECREATETABLES);
		    
		    log.info(SystemConfig.class, "EvaluationConfig.VIEWMANAGER_KILLDELAY:"+EvaluationConfig.VIEWMANAGER_KILLDELAY); 
		    

		    
		    
		    EvaluationConfig.EVALUATIONSETS = new ArrayList<EvaluationSet>();
		    List<String> evaluationSets = config.getList("evaluationSet.evaluationParams.iterations");
		    
		    for (int i = 0; i < evaluationSets.size(); i++) {
			
		    	System.out.println("asdfrwerwe");
			    
			    EvaluationParams evaluationParams=null;
			    List<VariableParam> params = new ArrayList<VariableParam>();
			    List<String> variableParams = config.getList("evaluationSet("+i+").evaluationParams.variableParam.name");
			    
			    
			    if(variableParams.size() != 0){
			    	
				    
			    	for (int j = 0; j < variableParams.size(); j++) {
						
					    String variableParamName = config.getString("evaluationSet("+i+").evaluationParams.variableParam("+j+").name");
					    String startValue = config.getString("evaluationSet("+i+").evaluationParams.variableParam("+j+").startValue");
					    String endValue = config.getString("evaluationSet("+i+").evaluationParams.variableParam("+j+").endValue");
					    String stepWidth = config.getString("evaluationSet("+i+").evaluationParams.variableParam("+j+").stepWidth");
					    int sw = ((stepWidth==null)?1:Integer.parseInt(stepWidth));
					    
					    
					    if(variableParamName != null && !variableParamName.equals("")){
							try {
								
								Experiment experiment = new Experiment();
								
								Class<?> c = experiment.getClass();
		
							    Field var = c.getDeclaredField(variableParamName);
							    var.set(experiment, i);
					
							    params.add(new VariableParam(variableParamName, Integer.parseInt(startValue), Integer.parseInt(endValue), sw));
							    
							   
							} catch (NoSuchFieldException e) {
								log.error(EvaluationConfig.class, e);
								variableParamName = null;
							} catch (SecurityException e) {
								log.error(EvaluationConfig.class, e);
								variableParamName = null;
							} catch (IllegalArgumentException e) {
								log.error(EvaluationConfig.class, e);
								variableParamName = null;
							} catch (IllegalAccessException e) {
								log.error(EvaluationConfig.class, e);
								variableParamName = null;
							} 
					    }
					    
			    	}
			    }

			    String iterations = config.getString("evaluationSet("+i+").evaluationParams.iterations");
			    
			    Integer it = (iterations != null)?Integer.parseInt(iterations):0;
			    
			    evaluationParams = new EvaluationParams(it, params);
    
			    String numOfRegionServers = config.getString("evaluationSet("+i+").experiment.numOfRegionServers");
			    String numOfViewManagers = config.getString("evaluationSet("+i+").experiment.numOfViewManagers");
			    String numOfClients = config.getString("evaluationSet("+i+").experiment.numOfClients");
			    String numOfKilledViewManagers = config.getString("evaluationSet("+i+").experiment.numOfKilledViewManagers");
			    
//			    List<String> basetables = config.getList("evaluationSet("+i+").experiment.createtables.basetable.name");
//			    List<String> aggregationView = config.getList("evaluationSet("+i+").experiment.createtables.aggregationView.name");
//			    List<String> selectionView = config.getList("evaluationSet("+i+").experiment.createtables.selectionView.name");
//			    List<String> indexView = config.getList("evaluationSet("+i+").experiment.createtables.indexView.name");
//			    List<String> deltaView = config.getList("evaluationSet("+i+").experiment.createtables.deltaView.name");
//			    List<String> reverseJoinView = config.getList("evaluationSet("+i+").experiment.createtables.reverseJoinView.name");
//			    List<String> joinView = config.getList("evaluationSet("+i+").experiment.createtables.joinView.name");
//			    
//			    log.info(EvaluationConfig.class, basetables.size()+" basetables found");
//			    log.info(EvaluationConfig.class, deltaView.size()+" delta views found");
//			    log.info(EvaluationConfig.class, aggregationView.size()+" aggregation views found");
//			    log.info(EvaluationConfig.class, selectionView.size()+" selection views found");
//			    log.info(EvaluationConfig.class, selectionView.size()+" index views found");
//			    log.info(EvaluationConfig.class, reverseJoinView.size()+" reverse join views found");
//			    log.info(EvaluationConfig.class, joinView.size()+" join views found");
			    
			    List<ICreateTable> createTables = new ArrayList<ICreateTable>();
			    
			    if(evaluationSets.size() != 0){
			    	
			    	for (int j = 0; j < tables.size(); j++) {
			    		Table table = tables.get(j);
			    		
			    		if ("basetable".equals(table.getType())) {
			    			String name = table.getName();
						    String numOfRegions = "2";
						    String numOfOperations = "50001";
						    String distribution = "uniform";
						    String useUpdates = "true";
						    String useDeletes = "true";
						    
						    CreateBaseTable createBaseTabe = new CreateBaseTable(name, distribution, Long.parseLong(numOfOperations), Integer.parseInt(numOfRegions), Boolean.parseBoolean(useUpdates), Boolean.parseBoolean(useDeletes));
						    
						    createTables.add(createBaseTabe);
						    
			    		} else if ("delta".equals(table.getType())) {
			    			String name = table.getName();
						    String type = table.getType();
						    String basetable = table.getBaseTables().get(0).getName();
						    
						    List<String> columns;
						    if (table.getPKPrefix().contains("k")) {
						    	columns = Arrays.asList("colAggKey1", "colAggVal1");
						    } else {
						    	columns = Arrays.asList("colAggKey2", "colAggVal2");
						    }
						    
						    String numOfRegions = "2";
						    String controlTable = null;
						    
						    CreateDeltaView createDeltaView = new CreateDeltaView(name, type, basetable, columns, Integer.parseInt(numOfRegions), controlTable);
				    
						    createTables.add(createDeltaView);
			    		} else if ("selection".equals(table.getType())) {
			    			String name = table.getName();
						    String basetable = table.getBaseTables().get(0).getName();
						    
						    // Add same columns as delta view into selection view
						    List<String> columns;
						    String selectionKey;
						    if (table.getPKPrefix().contains("k")) {
						    	selectionKey = "colAggVal1";
						    	columns = Arrays.asList("colAggKey1", "colAggVal1");
						    } else {
						    	selectionKey = "colAggVal2";
						    	columns = Arrays.asList("colAggKey2", "colAggVal2");
						    }
						    String selectionOperation = table.getFirstAttr();
						    String selectionValue = table.getSecondAttr();
						    String numOfRegions = "2";
						    // For view tester
						    String controlTable = null;
						    
						    CreateSelectionView createSelectionView = new CreateSelectionView(name, basetable, columns, selectionKey, selectionOperation, selectionValue, Integer.parseInt(numOfRegions), controlTable);
						    
						    createTables.add(createSelectionView);
			    		} else if ("sum".equals(table.getType()) || "count".equals(table.getType()) || "min".equals(table.getType()) || "max".equals(table.getType())) {
			    			String name = table.getName();
						    String type = table.getType();
						    String basetable = table.getBaseTables().get(0).getName();
						    String aggregationKey = table.getFirstAttr();
						    String aggregationValue = table.getSecondAttr();
						    String numOfRegions = "2";
						    String controlTable = null;
						    
						    CreateAggregationView createAggregationView = new CreateAggregationView(name, type, basetable, aggregationKey, aggregationValue, Integer.parseInt(numOfRegions), controlTable);
						    
						    createTables.add(createAggregationView);
			    		} else if ("reversejoin".equals(table.getType())) {
			    			String name = table.getName();
						    String numOfRegions = "2";
						    String controlTable = "";
		
		
						    List<Table> joinTableList = table.getBaseTables();
						    List<JoinTable> joinTables = new ArrayList<JoinTable>();
						    
						    if(joinTableList.size() != 0){
							
						    	
						    	for (int k = 0; k < joinTableList.size(); k++) {
								    String tableName = joinTableList.get(k).getName();
								    // TODO: change to aggregation key
								    String tableKey = "colAggKey" + (k+1);
								
								    
								    
								    JoinTable joinTable = new JoinTable(tableName, tableKey);
								    
//								    if(!leftTableKey.equals("primaryKey"))secondaryKeys.put(leftTable, leftTableKey);
//								    if(!rightTableKey.equals("primaryKey"))secondaryKeys.put(rightTable, rightTableKey);
								    
		
								    joinTables.add(joinTable);
								    
						    	}
//						    	log.info(EvaluationConfig.class,"secondaryKeys: "+secondaryKeys);
					    		
					    	}
		
						    
						    CreateReverseJoinView createJoinPairView = new CreateReverseJoinView(name, joinTables, Integer.parseInt(numOfRegions), controlTable);
						    createTables.add(createJoinPairView);
						    
			    			
			    		}
			    	}
			    
			    	
			    	

//			    
//			    if(indexView.size() != 0){
//				    
//			    	for (int j = 0; j < indexView.size(); j++) {
//						
//			    		
//					    String name = config.getString("evaluationSet("+i+").experiment.createtables.indexView("+j+").name");
//					    String basetable = config.getString("evaluationSet("+i+").experiment.createtables.indexView("+j+").basetable");
//					    String indexKey = config.getString("evaluationSet("+i+").experiment.createtables.indexView("+j+").indexKey");
//					    String numOfRegions = config.getString("evaluationSet("+i+").experiment.createtables.indexView("+j+").numOfRegions");
//					    String controlTable = config.getString("evaluationSet("+i+").experiment.createtables.indexView("+j+").controlTable");
//					    
//					    
//					    
//					    CreateIndexView createIndexView = new CreateIndexView(name, basetable, indexKey, Integer.parseInt(numOfRegions), controlTable);
//					    
//					    createTables.add(createIndexView);
//			    	} 
//						
//				}
			    
//			  if(reverseJoinView.size() != 0){
//			    
//		    	for (int j = 0; j < reverseJoinView.size(); j++) {
//					
//		    		
//				    String name = config.getString("evaluationSet("+i+").experiment.createtables.reverseJoinView("+j+").name");
//				    String numOfRegions = config.getString("evaluationSet("+i+").experiment.createtables.reverseJoinView("+j+").numOfRegions");
//				    String controlTable = config.getString("evaluationSet("+i+").experiment.createtables.reverseJoinView("+j+").controlTable");
//
//
//				    List<String> joinTableList = config.getList("evaluationSet("+i+").experiment.createtables.reverseJoinView.joinTables.joinTable.tableName");
////				    List<String> aggregationView = config.getList("evaluationSet("+i+").experiment.createtables.aggregationView.name");
//				    List<JoinTable> joinTables = new ArrayList<JoinTable>();
////				    Map<String, String> secondaryKeys = new HashMap<String, String>();
//				    
//				    if(joinTableList.size() != 0){
//					
//				    	
//				    	for (int k = 0; k < joinTableList.size(); k++) {
//				    		
//						    String tableName = config.getString("evaluationSet("+i+").experiment.createtables.reverseJoinView.joinTables.joinTable("+k+").tableName");
//						    String tableKey = config.getString("evaluationSet("+i+").experiment.createtables.reverseJoinView.joinTables.joinTable("+k+").tableKey");
//						
//						    
//						    
//						    JoinTable joinTable = new JoinTable(tableName, tableKey);
//						    
////						    if(!leftTableKey.equals("primaryKey"))secondaryKeys.put(leftTable, leftTableKey);
////						    if(!rightTableKey.equals("primaryKey"))secondaryKeys.put(rightTable, rightTableKey);
//						    
//
//						    joinTables.add(joinTable);
//						    
//				    	}
////				    	log.info(EvaluationConfig.class,"secondaryKeys: "+secondaryKeys);
//			    		
//			    	}
//
//				    
//				    CreateReverseJoinView createJoinPairView = new CreateReverseJoinView(name, joinTables, Integer.parseInt(numOfRegions), controlTable);
//				    createTables.add(createJoinPairView);
//		    	} 
//					
//			}
//			  
//			  
//			  if(joinView.size() != 0){
//				    
//			    	for (int j = 0; j < joinView.size(); j++) {
//						
//			    		
//					    String name = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").name");
//					    String joinPair = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").joinPairView");
//					    String numOfRegions = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").numOfRegions");
//					    String controlTable = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").controlTable");
//					    
//					    List<String> compositeKeyList = config.getList("evaluationSet("+i+").experiment.createtables.joinView.compositeKey.colFam");
////					    List<String> aggregationView = config.getList("evaluationSet("+i+").experiment.createtables.aggregationView.name");
//					    List<String> compositeKey = new ArrayList<String>();
////					    Map<String, String> secondaryKeys = new HashMap<String, String>();
//					    
//					    if(compositeKeyList.size() != 0){
//						
//					    	
//					    	for (int k = 0; k < compositeKeyList.size(); k++) {
//					    		
//							    String colFam = config.getString("evaluationSet("+i+").experiment.createtables.joinView.compositeKey.colFam("+k+")");
//		
//							    compositeKey.add(colFam);
//							    
//					    	}
////					    	log.info(EvaluationConfig.class,"secondaryKeys: "+secondaryKeys);
//				    		
//				    	}
//					    
//					    CreateJoinView createJoinView = new CreateJoinView(name, joinPair, compositeKey, Integer.parseInt(numOfRegions), controlTable);
//					    createTables.add(createJoinView);
//			    	} 
//						
//				}
			  
			  
			  
			  
			    
			   /* if(joinView.size() != 0){
				    
			    	for (int j = 0; j < joinView.size(); j++) {
						
			    		
					    String name = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").name");
//					    String useIndex = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").useIndex");
					    String numOfRegions = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").numOfRegions");
					    String controlTable = config.getString("evaluationSet("+i+").experiment.createtables.joinView("+j+").controlTable");

					    
					    
//					    
//					    String selectionValue = config.getString("evaluationSet("+i+").experiment.createtables.selectionView("+j+").selectionValue");
//					    String numOfRegions = config.getString("evaluationSet("+i+").experiment.createtables.selectionView("+j+").numOfRegions");
//					    String controlTable = config.getString("evaluationSet("+i+").experiment.createtables.selectionView("+j+").controlTable");
					    
//					    log.info(EvaluationConfig.class, name);
//					    log.info(EvaluationConfig.class, basetable);
//					    log.info(EvaluationConfig.class, selectionKey);
//					    log.info(EvaluationConfig.class, selectionOperation);
//					    log.info(EvaluationConfig.class, selectionValue);
//					    log.info(EvaluationConfig.class, numOfRegions);
//					    log.info(EvaluationConfig.class, controlTable);
					
					    
					    
					    
					    List<String> joinPairList = config.getList("evaluationSet("+i+").experiment.createtables.joinView.joinPairs.joinPair.leftTable");
//					    List<String> aggregationView = config.getList("evaluationSet("+i+").experiment.createtables.aggregationView.name");
					    List<JoinTablePair> joinPairs = new ArrayList<JoinTablePair>();
					    Map<String, String> secondaryKeys = new HashMap<String, String>();
					    
					    if(joinPairList.size() != 0){
						
					    	
					    	for (int k = 0; k < joinPairList.size(); k++) {
					    		
							    String leftTable = config.getString("evaluationSet("+i+").experiment.createtables.joinView.joinPairs.joinPair("+k+").leftTable");
//							    String leftTableKey = config.getString("evaluationSet("+i+").experiment.createtables.joinView.joinPairs.joinPair("+k+").leftTableJoinKey");
							    String rightTable = config.getString("evaluationSet("+i+").experiment.createtables.joinView.joinPairs.joinPair("+k+").rightTable");
//							    String rightTableKey = config.getString("evaluationSet("+i+").experiment.createtables.joinView.joinPairs.joinPair("+k+").rightTableJoinKey");
							    
							    
							    JoinTable left = new JoinTable(leftTable, leftTableKey);
							    JoinTable right = new JoinTable(rightTable, rightTableKey);
							    
							    if(!leftTableKey.equals("primaryKey"))secondaryKeys.put(leftTable, leftTableKey);
							    if(!rightTableKey.equals("primaryKey"))secondaryKeys.put(rightTable, rightTableKey);
							    
							    JoinTablePair joinPair = new JoinTablePair(left, right);
							    log.info(EvaluationConfig.class, joinPair.toString());

							    joinPairs.add(joinPair);
							    
					    	}
					    	log.info(EvaluationConfig.class,"secondaryKeys: "+secondaryKeys);
				    		
				    	}
					    

					    
					    CreateJoinView createJoinView = new CreateJoinView(name, joinPairs, Integer.parseInt(numOfRegions), controlTable);
					    createTables.add(createJoinView);
			    	} 
						
				}*/
			    
			    for (ICreateTable iCreateTable : createTables) {
			    	log.info(EvaluationConfig.class, iCreateTable.toString());
				}
				    
//				    String distribution = config.getString("evaluationSet("+i+").experiment.distribution");
//				    String viewTypes = config.getString("evaluationSet("+i+").experiment.viewTypes");
//				    String numOfViews = config.getString("evaluationSet("+i+").experiment.numOfViews");
//				    String numOfOperations = config.getString("evaluationSet("+i+").experiment.numOfOperations");
//				    String numOfRecords = config.getString("evaluationSet("+i+").experiment.numOfRecords");
//				    String numOfAggregationKeys = config.getString("evaluationSet("+i+").experiment.numOfAggregationKeys");
//				    String numOfBaseTableRegions = config.getString("evaluationSet("+i+").experiment.numOfBaseTableRegions");
//				    String numOfViewTableRegions = config.getString("evaluationSet("+i+").experiment.numOfViewTableRegions");
//				    String useDeletes = config.getString("evaluationSet("+i+").experiment.useDeletes");
//				    String useUpdates = config.getString("evaluationSet("+i+").experiment.useUpdates");
				    
				    
				    
				    
				    
				    
//				    List<String> viewTypesTemp = new ArrayList<String>();
//				    
//
//				    
//				    for (String viewType : viewTypes.split(",")) {
//				    	viewTypesTemp.add(viewType);
//				    }
//	
//	
//				    
				    Experiment experiment = new Experiment(Integer.parseInt(numOfRegionServers), Integer.parseInt(numOfViewManagers), Integer.parseInt(numOfClients), 
				    		Integer.parseInt(numOfKilledViewManagers), createTables); ;
				    
				    EvaluationSet evaluationSet = new EvaluationSet(evaluationParams, experiment);
	
				    EvaluationConfig.EVALUATIONSETS.add(evaluationSet);
			    
			    }
			    

			}
		    
		    int numOfExperiments = 0;
		    for (EvaluationSet evaluationSet : EvaluationConfig.EVALUATIONSETS) {
				
		    	numOfExperiments += evaluationSet.getEvaluationParams().getIterations();
		    	log.info(EvaluationConfig.class, evaluationSet.toString());
			}
		    
		    log.info(EvaluationConfig.class, numOfExperiments+" experiments found");
		    
		    
		}
		catch(ConfigurationException cex)
		{
			
			log.error(EvaluationConfig.class, cex);
			cex.printStackTrace();
			System.exit(0);
		} 
		

		log.info(EvaluationConfig.class, "--------------------------------------------------------------");
  



	}
	

	

	
	
	public static void main(String[] args){
//		 load();
	}
	

}
