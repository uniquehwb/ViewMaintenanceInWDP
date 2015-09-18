package de.webdataplatform.viewmanager.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.BytesUtil;
import de.webdataplatform.settings.CreateAggregationView;
import de.webdataplatform.settings.CreateDeltaView;
import de.webdataplatform.settings.CreateIndexView;
import de.webdataplatform.settings.CreateReverseJoinView;
import de.webdataplatform.settings.CreateJoinView;
import de.webdataplatform.settings.CreateSelectionView;
import de.webdataplatform.settings.JoinTable;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.view.OperationMode;
import de.webdataplatform.view.TableService;
import de.webdataplatform.view.ViewMode;
import de.webdataplatform.viewmanager.CommitLog;
import de.webdataplatform.viewmanager.ICommitLog;

public class Processing implements Runnable{




	private Queue<BaseTableUpdate> incomingQueue;
	

	
	
//	
//	private QueryBaseTable queryBaseTable;
//	
//	private CalculateResult calculateResult;
//	
//	private UpdateView updateView;
//	
	private ICommitLog commitLog;
	
	
	private String vmName;
	
	private ConcurrentHashMap<String, String> markers;
	
	

	private TableService tableService;


	private Log log;
	

	
	public Processing(Log log, Queue<BaseTableUpdate> incomingQueue, String vmName){
		this.incomingQueue = incomingQueue;
		this.vmName = vmName;
		this.commitLog = new CommitLog(log, vmName);
		viewRecordUpdates = new AtomicLong();
		commitLogUpdates = new AtomicLong();
		markers = new ConcurrentHashMap<String, String>();
		this.log = log;
		
		tableService = new TableService(log);

	}
	
	private long lastMeasure;
//	private AtomicLong insertCount;
//	private AtomicLong updateCount;
//	private AtomicLong deletionCount;
	private AtomicLong viewRecordUpdates;
	private AtomicLong commitLogUpdates;
	
	@Override
	public void run() {
		

		
		while(true){
			
			try{
			
			long currentTime = new Date().getTime();
			if((currentTime - lastMeasure) > SystemConfig.VIEWMANAGER_DISPLAYINTERVAL){
				lastMeasure = currentTime;
				log.info(this.getClass()," ---------------------");
				log.info(this.getClass()," load currently at "+incomingQueue.size());
				log.info(this.getClass()," updates processed "+viewRecordUpdates.get());
				log.info(this.getClass()," ---------------------");
			}
//			long pollingTime = new Date().getTime();
			BaseTableUpdate update = incomingQueue.poll();
//			log.performance(this.getClass(), "polling time: "+(new Date().getTime() - pollingTime));
			
			if(update != null){
				if(SystemConfig.LOGGING_LOGUPDATES)log.info(this.getClass(),"processing update: "+update);
				log.performance(this.getClass(), "----------------------------");
				try{
						
					if(update.getBaseTable().startsWith(SystemConfig.MESSAGES_MARKERPREFIX)){
						log.info(this.getClass(),"marker received, putting to map: "+update);
						markers.put(update.getBaseTable(), update.getRegionServer());
						continue;
					}
					// Add finish marker to view table
					if(!update.getViewTable().equals("zfinish_marker") && update.getKey().contains("finishmarker")){

						tableService.put(Bytes.toBytes(update.getViewTable()), Bytes.toBytes(update.getKey()), Bytes.toBytes("colfam1"), BytesUtil.convertMap(update.getColumns()), null);
						continue;
					}
					// Add finish marker to finish_markers table
					if(update.getViewTable().equals("zfinish_marker")){

						if(update.getKey().contains("finishmarker")){
							tableService.put(Bytes.toBytes("finish_markers"), Bytes.toBytes(update.getKey()), Bytes.toBytes("colfam1"), BytesUtil.convertMap(update.getColumns()), null);
						}
						continue;
					}
					
					long processingTime = new Date().getTime();
					process(update);
					log.performance(this.getClass(), "processing time: "+(new Date().getTime() - processingTime));

					viewRecordUpdates.incrementAndGet();
					
//					log.info(this.getClass(),"writing update to commit log: "+update);
					
					long walTime = new Date().getTime();
//					if(SystemConfig.FAULTTOLERANCE_COMMITLOG){
		
						//Commit Table
						
//						commitLog.append(vmName, update);
//					}
					
					
//					if(update.getType().equals("Put")){
//						Map<byte[], byte[]> commitRecord = BytesUtil.convertMap(update.getColumns());
//						tableService.put(Bytes.toBytes(update.getBaseTable()+"_commit"), Bytes.toBytes(update.getKey()), commitRecord);
//					}
//					
//					if(update.getType().equals("Delete") || update.getType().equals("DeleteColumn") || update.getType().equals("DeleteFamily")){
//						
//						List<byte[]> columns = BytesUtil.convertMapToList(BytesUtil.convertMap(update.getColumns()));
//						tableService.delete(Bytes.toBytes(update.getBaseTable()+"_commit"), Bytes.toBytes(update.getKey()), columns);
//					}
//						
//						
					

					
					commitLogUpdates.incrementAndGet();
					log.performance(this.getClass(), "walTime time: "+(new Date().getTime() - walTime));
					
				}catch(Exception e){
					
					log.info(this.getClass(), "errornous update:"+update.convertToString());
					log.error(this.getClass(), e);
		
				}
				
			}
			
		
			if(incomingQueue.size() == 0){
			try {
				Thread.sleep(SystemConfig.VIEWMANAGER_UPDATEPOLLINGINTERVAL);
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			}
			}
			
			}catch(Exception e){
				log.error(this.getClass(), e);
			}
		}
		
	}
	

	
	
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#pollUpdates()
	 */
	public void process(BaseTableUpdate btu){
		
			propagateUpdate(btu);
		
	}
	
	

	

	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateUpdate(java.lang.String, de.webdataplatform.storage.BaseTableColumn, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateUpdate(BaseTableUpdate btu){
		

		String region = btu.getRegion();
		String seqNo = btu.getSeqNo();
		String signature = region+"_"+seqNo;
		
//		setOldBaseRecord(btu);
		
		
		btu.setColumns(btu.getColumnsWithoutSignatures());
		btu.setOldColumns(btu.getOldColumnsWithoutSignatures());
		
		log.updates(this.getClass(), "btu: "+btu);
		log.performance(this.getClass(), "btu: "+btu);
		
//		if(btu.getColumns().isEmpty() && btu.getOldColumns().isEmpty())return;

		
		if(btu.getViewMode().equals(ViewMode.AGGREGATION_COUNT) || 
				btu.getViewMode().equals(ViewMode.AGGREGATION_SUM) ||
				btu.getViewMode().equals(ViewMode.AGGREGATION_MIN) ||
				btu.getViewMode().equals(ViewMode.AGGREGATION_MAX) ||
				btu.getViewMode().equals(ViewMode.INDEX)) {
			
			// When join key changes, all reverse join pairs related to the old join key should be 
			// set to empty.
			if (containsNullValues(btu.getColumns())) {
				Map<String, String> result = new HashMap<String, String>();
				btu.setColumns(result);
			}
			
//			btu.setColumns(removeNullValues(btu.getColumns()));
			btu.setOldColumns(removeNullValues(btu.getOldColumns()));
			
			// Base table of aggregation view is not join.
			if (!btu.getBaseTable().contains("join")) {
				if(btu.getType().equals("Put")){
					if(btu.getColumns().isEmpty() && !btu.getOldColumns().isEmpty()){
						// Operations are always "put" in WAL, here changing to "delete" is 
						// just for internal update.
						btu.setType("Delete");
						
					}
				}
			}
			
		}
		
		if(btu.getViewMode().equals(ViewMode.SELECTION)) {
			btu.setColumns(removeNullValues(btu.getColumns()));
			btu.setOldColumns(removeNullValues(btu.getOldColumns()));
			
			if(btu.getType().equals("Put")){
				if(btu.getColumns().isEmpty() && !btu.getOldColumns().isEmpty()){
					// Operations are always "put" in WAL, here changing to "delete" is 
					// just for internal update.
					btu.setType("Delete");
					
				}
			}
		}
				
		if(btu.getViewMode().equals(ViewMode.JOIN)){
			
//				log.info(this.getClass(), "Nullvalues:"+containsNullValues(btu.getColumns()));
				if(containsNullValues(btu.getColumns())){
					btu.setType("Delete");
//					log.info(this.getClass(), "Nullvalues:"+btu.getColumns());
					
				}
			
		}
			
		if(btu.getType().equals("Put")){

			if (btu.getViewMode().equals(ViewMode.REVERSE_JOIN)) {
				// Check whether join key changed
				CreateReverseJoinView cJPV = CreateReverseJoinView.parse(btu.getViewDefinition());
				String newJoinKey = "";
				String oldJoinKey = "";
				Map<String, String> columns = btu.getColumns();
				Map<String, String> oldColumns = btu.getOldColumns();
				// Check join key in all join tables.
				for (JoinTable joinTable: cJPV.getJoinTables()){
					if (columns != null && columns.get(joinTable.getJoinKey()) != null && !columns.get(joinTable.getJoinKey()).isEmpty()) {
						newJoinKey = columns.get(joinTable.getJoinKey());
					} 
					if (oldColumns != null && oldColumns.get(joinTable.getJoinKey()) != null && !oldColumns.get(joinTable.getJoinKey()).isEmpty()) {
						oldJoinKey = oldColumns.get(joinTable.getJoinKey());
					}
					
					// wrong join table
					if (newJoinKey.equals("") && oldJoinKey.equals("")) {
						continue;
					}
					
					// join key changed
					if (!newJoinKey.equals("") && !oldJoinKey.equals("") && !newJoinKey.equals(oldJoinKey)) {
						// split to a delete and an insert
						BaseTableUpdate btu1 = btu.copy();
						BaseTableUpdate btu2 = btu.copy();
						for (String column : columns.keySet()) {
							columns.put(column, "");
						}
						btu1.setColumns(columns);
						propagate(btu1, OperationMode.INSERT, signature);
						
						for (String oldColumn : oldColumns.keySet()) {
							oldColumns.put(oldColumn, "");
						}
						btu2.setOldColumns(oldColumns);
						propagate(btu2, OperationMode.INSERT, signature + "_1");
					} else {
						long insertionTime = new Date().getTime();
						propagate(btu, OperationMode.INSERT, signature);
						log.performance(this.getClass(), "insertionTime time: "+(new Date().getTime() - insertionTime));
					}
				}
				
			}
			else if (btu.getViewMode().equals(ViewMode.AGGREGATION_SUM) || btu.getViewMode().equals(ViewMode.AGGREGATION_COUNT)) 
			{
				if (btu.getBaseTable().contains("join")) {
					CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
					String keyName = cAV.getAggregationKey();
					
					Map<String, String> columns = btu.getColumns();
					Map<String, String> oldColumns = btu.getOldColumns();
					Map<String, String> comKeyAndAggrKeyMapNew = new HashMap<String, String>();
					Map<String, String> comKeyAndAggrKeyMapOld = new HashMap<String, String>();
					
					// Get map of composite key and aggregation key.
					if (columns != null && !columns.isEmpty()) {
						for (String column: columns.keySet()) {
							String compositeKey = column.split("_")[0];
							if (compositeKey.contains("k") && compositeKey.contains("l") && column.contains(keyName) && !comKeyAndAggrKeyMapNew.containsKey(compositeKey)) {
								comKeyAndAggrKeyMapNew.put(compositeKey, columns.get(column));
							}
						}
					}
					if (oldColumns != null && !oldColumns.isEmpty()) {
						for (String oldColumn: oldColumns.keySet()) {
							String compositeKey = oldColumn.split("_")[0];
							if (compositeKey.contains("k") && compositeKey.contains("l") && oldColumn.contains(keyName) && !comKeyAndAggrKeyMapOld.containsKey(compositeKey)) {
								comKeyAndAggrKeyMapOld.put(compositeKey, oldColumns.get(oldColumn));
							}
						}
					}
					
					// update
					if (comKeyAndAggrKeyMapNew != null && !comKeyAndAggrKeyMapNew.isEmpty() && comKeyAndAggrKeyMapOld != null && !comKeyAndAggrKeyMapOld.isEmpty()) 
					{
						int i = 1;
						for (String comKey: comKeyAndAggrKeyMapNew.keySet()) {
							// Aggregation key changed
							if (!comKeyAndAggrKeyMapNew.get(comKey).equals(comKeyAndAggrKeyMapOld.get(comKey))) {
								// Generate a first delete
								String oldAggrKey = comKeyAndAggrKeyMapOld.get(comKey);
								BaseTableUpdate aggrBtu1 = btu.copy();
								Map<String, String> aggrColumns1 = new HashMap<String, String>();
								Map<String, String> aggrOldColumns1 = new HashMap<String, String>();
								
								if (oldColumns != null && !oldColumns.isEmpty()) {
									for (String oldColumn: oldColumns.keySet()) { 
										if (oldColumn.contains(comKey)) {
											aggrOldColumns1.put(oldColumn, oldColumns.get(oldColumn));
										}
									}
								}
								aggrBtu1.setKey(oldAggrKey);
								aggrBtu1.setColumns(aggrColumns1);
								aggrBtu1.setOldColumns(aggrOldColumns1);
								
								log.info(this.getClass(), "aggrBtu1: " + aggrBtu1);
								
								propagate(aggrBtu1, OperationMode.DELETE, signature + "_" + i);
								i++;
								
								// Generate a second insert
								String newAggrKey = comKeyAndAggrKeyMapNew.get(comKey);
								BaseTableUpdate aggrBtu2 = btu.copy();
								Map<String, String> aggrColumns2 = new HashMap<String, String>();
								Map<String, String> aggrOldColumns2 = new HashMap<String, String>();
								
								if (columns != null && !columns.isEmpty()) {
									for (String column: columns.keySet()) { 
										if (column.contains(comKey)) {
											aggrColumns2.put(column, columns.get(column));
										}
									}
								}
								aggrBtu2.setKey(newAggrKey);
								aggrBtu2.setColumns(aggrColumns2);
								aggrBtu2.setOldColumns(aggrOldColumns2);
								
								log.info(this.getClass(), "aggrBtu2: " + aggrBtu2);
								
								propagate(aggrBtu2, OperationMode.INSERT, signature + "_" + i);
								i++;
							} 
							// Aggregation key not changed
							else 
							{
								String newAggrKey = comKeyAndAggrKeyMapNew.get(comKey);
								BaseTableUpdate aggrBtu = btu.copy();
								Map<String, String> aggrColumns = new HashMap<String, String>();
								Map<String, String> aggrOldColumns = new HashMap<String, String>();
								
								if (oldColumns != null && !oldColumns.isEmpty()) {
									for (String oldColumn: oldColumns.keySet()) { 
										if (oldColumn.contains(comKey)) {
											aggrOldColumns.put(oldColumn, oldColumns.get(oldColumn));
										}
									}
								}
								
								if (columns != null && !columns.isEmpty()) {
									for (String column: columns.keySet()) { 
										if (column.contains(comKey)) {
											aggrColumns.put(column, columns.get(column));
										}
									}
								}
								aggrBtu.setKey(newAggrKey);
								aggrBtu.setColumns(aggrColumns);
								aggrBtu.setOldColumns(aggrOldColumns);
								
								log.info(this.getClass(), "aggrBtu: " + aggrBtu);
								
								propagate(aggrBtu, OperationMode.INSERT, signature + "_" + i);
								i++;
							}
						}
					}
					// insert
					else if (comKeyAndAggrKeyMapNew != null && !comKeyAndAggrKeyMapNew.isEmpty())
					{
						int i = 1;
						for (String comKey: comKeyAndAggrKeyMapNew.keySet()) {
							String newAggrKey = comKeyAndAggrKeyMapNew.get(comKey);
							BaseTableUpdate aggrBtu = btu.copy();
							Map<String, String> aggrColumns = new HashMap<String, String>();
							Map<String, String> aggrOldColumns = new HashMap<String, String>();
							
							if (columns != null && !columns.isEmpty()) {
								for (String column: columns.keySet()) { 
									if (column.contains(comKey)) {
										aggrColumns.put(column, columns.get(column));
									}
								}
							}
							aggrBtu.setKey(newAggrKey);
							aggrBtu.setColumns(aggrColumns);
							aggrBtu.setOldColumns(aggrOldColumns);
							
							log.info(this.getClass(), "aggrBtu: " + aggrBtu);
							
							propagate(aggrBtu, OperationMode.INSERT, signature + "_" + i);
							i++;
						}
					}
					// delete
					else if (comKeyAndAggrKeyMapOld != null && !comKeyAndAggrKeyMapOld.isEmpty()) 
					{
						int i = 1;
						for (String comKey: comKeyAndAggrKeyMapOld.keySet()) {
							String oldAggrKey = comKeyAndAggrKeyMapOld.get(comKey);
							BaseTableUpdate aggrBtu = btu.copy();
							Map<String, String> aggrColumns = new HashMap<String, String>();
							Map<String, String> aggrOldColumns = new HashMap<String, String>();
							
							if (oldColumns != null && !oldColumns.isEmpty()) {
								for (String oldColumn: oldColumns.keySet()) { 
									if (oldColumn.contains(comKey)) {
										aggrOldColumns.put(oldColumn, oldColumns.get(oldColumn));
									}
								}
							}
							aggrBtu.setKey(oldAggrKey);
							aggrBtu.setColumns(aggrColumns);
							aggrBtu.setOldColumns(aggrOldColumns);
							
							log.info(this.getClass(), "aggrBtu: " + aggrBtu);
							
							propagate(aggrBtu, OperationMode.DELETE, signature + "_" + i);
							i++;
						}
					}
				} else {
					// Check whether aggregation key changed
					CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
					String aggrCol = cAV.getAggregationKey();
					String newAggrKey = "";
					String oldAggrKey = "";
					Map<String, String> columns = btu.getColumns();
					Map<String, String> oldColumns = btu.getOldColumns();
					
					if (columns != null && columns.get(aggrCol) != null && !columns.get(aggrCol).isEmpty()) {
						newAggrKey = columns.get(aggrCol);
					} 
					if (oldColumns != null && oldColumns.get(aggrCol) != null && !oldColumns.get(aggrCol).isEmpty()) {
						oldAggrKey = oldColumns.get(aggrCol);
					}
					
					// aggregation key changed
					if (!newAggrKey.equals("") && !oldAggrKey.equals("") && !newAggrKey.equals(oldAggrKey)) {
						// split to a delete and an insert
						BaseTableUpdate btu1 = btu.copy();
						BaseTableUpdate btu2 = btu.copy();
						for (String column : columns.keySet()) {
							columns.put(column, "");
						}
						btu1.setColumns(columns);
						propagate(btu1, OperationMode.DELETE, signature);
						log.info(this.getClass(), "btu1: " + btu1);
						
						for (String oldColumn : oldColumns.keySet()) {
							oldColumns.put(oldColumn, "");
						}
						btu2.setOldColumns(oldColumns);
						log.info(this.getClass(), "btu2: " + btu2);
						propagate(btu2, OperationMode.INSERT, signature + "_1");
					} else {
						long insertionTime = new Date().getTime();
						propagate(btu, OperationMode.INSERT, signature);
						log.performance(this.getClass(), "insertionTime time: "+(new Date().getTime() - insertionTime));
					}
				}
			}
			else
			{
				long insertionTime = new Date().getTime();
				propagate(btu, OperationMode.INSERT, signature);
				log.performance(this.getClass(), "insertionTime time: "+(new Date().getTime() - insertionTime));
			}
		}
	
		if(btu.getType().equals("Delete") || btu.getType().equals("DeleteColumn") || btu.getType().equals("DeleteFamily")){

			propagate(btu, OperationMode.DELETE, signature);
		}
			
		
		

		
		
		
//		if(btu.getPropagationMode().equals(PropagationMode.INSERT))propagate(btu, PropagationMode.INSERT, signature);
//		
//		if(btu.getPropagationMode().equals(PropagationMode.UPDATE)){
//			propagate(btu, PropagationMode.DELETE, signature+"_1");
//			propagate(btu, PropagationMode.INSERT, signature+"_2");
//		}
//		if(btu.getPropagationMode().equals(PropagationMode.DELETE))propagate(btu, PropagationMode.DELETE, signature);
		

		log.updates(this.getClass(), "-------------------------------");
		
		
		
	}
	
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateInsert(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagate(BaseTableUpdate btu, OperationMode propagationMode, String signature) {

		ViewMode viewMode = ViewMode.valueOf(btu.getViewDefinition().split(",")[0]);
//		log.info(this.getClass(), "Calculating "+propagationMode.toString()+" "+viewMode.toString());

		String key="";
		List<String> joinKeys;
//		String checkQualifier="";
//		String checkValue="";

		
		List<byte[]> getColumns = new ArrayList<byte[]>();
		List<String> colFams = null;
		
		
		
		long getKeyTime = new Date().getTime();	
		key = getViewRecordKey(btu, propagationMode);
		
		if(viewMode.equals(ViewMode.JOIN)){
			
			joinKeys = new ArrayList<String>();
			
		}
		
		log.info(this.getClass(), "key:"+key);
//		log.info(this.getClass(), "key:"+key);
		log.performance(this.getClass(), "getKey time: "+(new Date().getTime() - getKeyTime));
		
		getColumns = getViewRecordColumns(btu);
		colFams = getViewRecordColFam(btu);
		
//		log.info(this.getClass(), "colFam:"+colFam);
//		getList.add(Bytes.toBytes(signature));
		
//		log.info(this.getClass(), "getList:"+BytesUtil.listToString(getColumns));
//		log.info(this.getClass(), "ViewTable:"+btu.getViewTable());
		
		Boolean succeed = false;
		Result oldViewRecord=null;


		do{
			
			long retrivalTime = new Date().getTime();
			oldViewRecord = retrieveViewRecord(btu, key, BytesUtil.convertList(colFams), getColumns, Bytes.toBytes(signature));
			log.performance(this.getClass(), "retrivalTime time: "+(new Date().getTime() - retrivalTime));
			
//			log.info(this.getClass(), "oldViewRecord: "+BytesUtil.convertMapBack(oldViewRecord));
			
			boolean hasProcessed;
			if(oldViewRecord == null)hasProcessed = false;
			else hasProcessed = BytesUtil.mapContains(Bytes.toBytes(signature), oldViewRecord.getFamilyMap(Bytes.toBytes("sigfam1")));
			
			
//			System.out.println("oldViewRecord:"+BytesUtil.mapToString(oldViewRecord));
//			System.out.println("hasProcessed: "+hasProcessed);

				
			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
				if(hasProcessed){
					log.info(this.getClass(),"Update key="+key+", signature="+signature+" rejected --> It has already been processed");
					break;
				}
			}
				
			long updateView = new Date().getTime();	
			succeed = updateView(btu, propagationMode, key, colFams, oldViewRecord, signature);
			log.performance(this.getClass(), "updateView time: "+(new Date().getTime() - updateView));
			
			
		   
			if(succeed == null)break;	
			if(!succeed)log.info(this.getClass(), "Concurrent Update failed");
			 
			
		}while(!succeed);
		
	}
	
	private List<String> getViewRecordColFam(BaseTableUpdate btu) {
		
		ViewMode viewMode = btu.getViewMode();
		List<String> colFams = new ArrayList<String>();
		
		
		if(!viewMode.equals(ViewMode.REVERSE_JOIN)){
			
			colFams.add("colfam1");
		}
		
		if(viewMode.equals(ViewMode.REVERSE_JOIN)){

			String baseTable = btu.getBaseTable();
//			String colFamBaseRecord = btu.getColumnFamily();
			
					
					
			CreateReverseJoinView cRJV = CreateReverseJoinView.parse(btu.getViewDefinition());
			
			for (JoinTable joinTable : cRJV.getJoinTables()) {
				
				if(!btu.getBaseTable().equals(joinTable.getTableName())){
					colFams.add(joinTable.getTableName()+"fam1");
				}
				
			}
			colFams.add("countfam1");
			colFams.add("joinfam1");
			
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getLeftTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getRightTable().getTableName()+"fam1";
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getRightTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getLeftTable().getTableName()+"fam1";
//			colFam = Bytes.toBytes(btu.getBaseTable());
			
		}
		
//		log.info(this.getClass(), "colFams:"+colFams);
		return colFams;
	}
	
//	private String getBaseRecordColFam(BaseTableUpdate btu) {
//		
//		ViewMode viewMode = btu.getViewMode();
//		String colFam = null;
//
//		colFam = "colfam1";
//		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
//			String baseTable = btu.getBaseTable();
//			CreateReverseJoinView cJP = CreateReverseJoinView.parse(btu.getViewDefinition());
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getLeftTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getLeftTable().getTableName()+"fam1";
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getRightTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getRightTable().getTableName()+"fam1";
//		
//		}
//		
//		return colFam;
//	}
	
	

	private String getViewRecordKey(BaseTableUpdate btu, OperationMode propagationMode) {
		
		String key = "";
		
		ViewMode viewMode = btu.getViewMode();
//		System.out.println("Calculating "+propagationMode.toString()+" "+viewMode.toString());


		Map<String, String> columns=null;
		Map<String, String> oldColumns=null;
		
		if(propagationMode.equals(OperationMode.INSERT)) {
			columns = btu.getColumns(); 
			oldColumns = btu.getOldColumns();
		}
		if(propagationMode.equals(OperationMode.DELETE)) {
			columns = btu.getOldColumns(); 
		}
		
		

		if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
		
			CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
			
			// Previous view is not join view, namely pk of the previous view is pk of base table.
			if (columns.get(cAV.getAggregationKey()) != null) {
				key=columns.get(cAV.getAggregationKey());
			} else {
				key = btu.getKey();
			}
			
		}
		if(viewMode.equals(ViewMode.SELECTION)){
			
			key = btu.getKey();
			

		}
		
		if(viewMode.equals(ViewMode.INDEX)){
			CreateIndexView cIV = CreateIndexView.parse(btu.getViewDefinition());
			key = columns.get(cIV.getIndexKey());
	
		}
		
		if(viewMode.equals(ViewMode.DELTA)){
			key = btu.getKey();
	
		}
		
		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
			
			CreateReverseJoinView cJPV = CreateReverseJoinView.parse(btu.getViewDefinition());
			// Check join key in all join tables.
			for (JoinTable joinTable: cJPV.getJoinTables()){
				log.updates(this.getClass(), "joinTable: "+joinTable);
				if (columns != null && columns.get(joinTable.getJoinKey()) != null && !columns.get(joinTable.getJoinKey()).isEmpty()) {
					key = columns.get(joinTable.getJoinKey());
					return key;
				} 
				// For delete, join key only exists in old columns.
				if (oldColumns != null && oldColumns.get(joinTable.getJoinKey()) != null && !oldColumns.get(joinTable.getJoinKey()).isEmpty()) {
					key = oldColumns.get(joinTable.getJoinKey());
					return key;
				}
			}
		}
		if(viewMode.equals(ViewMode.JOIN)){
			
			
		}
		

		return key;
	}



	
	private List<byte[]> getViewRecordColumns(BaseTableUpdate btu) {
		
		ViewMode viewMode = btu.getViewMode();
		List<byte[]> getList = new ArrayList<byte[]>();
		
		if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
			
			CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
			getList.add(Bytes.toBytes(cAV.getAggregationValue()+"_new"));
		}
		//TODO haha
		if(viewMode.equals(ViewMode.DELTA) ){
			
			CreateDeltaView cDV = CreateDeltaView.parse(btu.getViewDefinition());
			
			for(String key : cDV.getColumns()){
				
				getList.add(Bytes.toBytes(key+"_new"));
			}
			
		}
		if(viewMode.equals(ViewMode.SELECTION) ){
			
			CreateSelectionView cSV = CreateSelectionView.parse(btu.getViewDefinition());
			
			for(String key : cSV.getColumns()){
				
				getList.add(Bytes.toBytes(key+"_new"));
			}
			
		}

		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
			getList=null;
		}
		
		return getList;
	}
	




	public Result retrieveViewRecord(BaseTableUpdate btu, String viewRecordKey, List<byte[]> colFams, List<byte[]> cols, byte[] signature){
		
		
		if((viewRecordKey == null || viewRecordKey.equals(""))){
			
			return Result.EMPTY_RESULT;
		}

		return tableService.get(Bytes.toBytes(btu.getViewTable()), Bytes.toBytes(viewRecordKey), colFams, cols, signature);
	}

	
	private Boolean updateView(BaseTableUpdate btu, OperationMode propagationMode, String viewRecordKey, List<String> colFams, Result oldVM, String signature){
		log.updates(this.getClass(), "viewRecordKey: "+viewRecordKey);
		ViewMode viewMode = btu.getViewMode();
		String viewTableName = btu.getViewTable();
		Boolean succeed=false;
		Map<String, String> columns=null;
		Map<String, String> oldColumns = null;
		Map<byte[], byte[]> newViewRecord = new HashMap<byte[], byte[]>();
		List<byte[]> deleteViewRecord = new ArrayList<byte[]>();

		
		columns = btu.getColumns(); 
		oldColumns = btu.getOldColumns();
		
//		if(propagationMode.equals(PropagationMode.INSERT))columns = btu.getColumns(); 
//		if(propagationMode.equals(PropagationMode.DELETE))columns = btu.getOldColumns(); 
		
//		log.info(this.getClass(), "columns: "+columns);
		
		//TODO: should dismiss update triggered by table doesn't contain aggregation value
		if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
			
			// Ignore empty update
			if (viewRecordKey == null || viewRecordKey.isEmpty()) {
				return true;
			}
			
			Map<byte[], byte[]> oldViewRecord=null;
			
			if(oldVM != null)oldViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(0)));
			
			CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
			log.updates(this.getClass(), "oldVM: "+oldVM);
			log.updates(this.getClass(), "colFams: "+colFams);
			log.updates(this.getClass(), "oldViewRecord: "+oldViewRecord);
			
			String keyName = cAV.getAggregationKey();
			String valueName = cAV.getAggregationValue();
			
			
			Long oldValue = null;
			if(oldViewRecord != null && oldViewRecord.get(Bytes.toBytes(valueName+"_new")) != null){
				oldValue = Long.parseLong(Bytes.toString(oldViewRecord.get(Bytes.toBytes(valueName+"_new"))));
			}else{
				oldValue = 0l;
			}
			log.updates(this.getClass(), "oldValue: "+oldValue);
			Long deltaValue=0l;
			Long deltaCount= 0l;
			if(propagationMode.equals(OperationMode.INSERT))
				// Previous view is not join view, namely pk of the previous view is pk of base table.
				if (columns.get(cAV.getAggregationValue()) != null) 
				{
					deltaValue=Long.parseLong(columns.get(cAV.getAggregationValue()));
					// If there is an old value:
					// for sum, should minus it to get correct delta value;
					// for count, should not change the count number.
					if (oldColumns.get(cAV.getAggregationValue()) != null && !oldColumns.get(cAV.getAggregationValue()).equals(""))
					{
						deltaValue = deltaValue - Long.parseLong(oldColumns.get(cAV.getAggregationValue()));
						deltaCount = 0l;
					}
					log.updates(this.getClass(), "deltaValue: "+deltaValue);
				} 
				// previous view is join view, pk of both views are aggregation key.
				else 
				{
					for (String column: columns.keySet()) {
						// First part of column name, which is composite key or pk and 
						// pk should be ignored.
						String prefix = column.split("_")[0];
						if (prefix.contains("k") && prefix.contains("l") && column.contains(cAV.getAggregationValue())) {
							deltaValue += Long.parseLong(columns.get(column));
							deltaCount++;
						}
					}
					for (String oldColumn: oldColumns.keySet()) {
						// First part of column name, which is composite key or pk and 
						// pk should be ignored.
						String prefix = oldColumn.split("_")[0];
						if (prefix.contains("k") && prefix.contains("l") && oldColumn.contains(cAV.getAggregationValue())) {
							deltaValue -= Long.parseLong(oldColumns.get(oldColumn));
							deltaCount--;
						}
					}
				}
			if(propagationMode.equals(OperationMode.DELETE)) {
				if (oldColumns.get(cAV.getAggregationValue()) != null) {
					deltaValue=Long.parseLong(oldColumns.get(cAV.getAggregationValue()));
					deltaCount++;
				} else {
					for (String oldColumn: oldColumns.keySet()) {
						String prefix = oldColumn.split("_")[0];
						if (prefix.contains("k") && prefix.contains("l") && oldColumn.contains(valueName)) {
							deltaValue += Long.parseLong(oldColumns.get(oldColumn));
							deltaCount++;
						}
					}
				}
			}
			
			Long result = null;
			
			if(viewMode.equals(ViewMode.AGGREGATION_SUM)){
				
				if(propagationMode.equals(OperationMode.INSERT))result = oldValue + deltaValue;
				if(propagationMode.equals(OperationMode.DELETE))result = oldValue - deltaValue;
				
			}
			if(viewMode.equals(ViewMode.AGGREGATION_COUNT)){
				if(propagationMode.equals(OperationMode.INSERT))result = oldValue + deltaCount;
				if(propagationMode.equals(OperationMode.DELETE))result = oldValue - deltaCount;
			}
			if(viewMode.equals(ViewMode.AGGREGATION_MIN)){
				
				if(propagationMode.equals(OperationMode.INSERT)){
					
					newViewRecord.put(Bytes.toBytes(btu.getKey()), Bytes.toBytes(String.valueOf(deltaValue)));
					if(oldValue == 0)oldValue = Long.MAX_VALUE;
					if(deltaValue < oldValue){
//						log.info(this.getClass(), "oldvalue: "+oldValue);
//						log.info(this.getClass(), "deltaValue: "+deltaValue);
						result = deltaValue;
					}
					else result = oldValue;
				}
				if(propagationMode.equals(OperationMode.DELETE)){
					
					deleteViewRecord.add(Bytes.toBytes(String.valueOf(btu.getKey())));
					deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, deleteViewRecord, signature);
					deleteViewRecord = new ArrayList<byte[]>();
					
					log.updates(this.getClass(), "deltaValue: "+deltaValue);
					log.updates(this.getClass(), "oldValue: "+oldValue);
					log.updates(this.getClass(), "equals: "+deltaValue.equals(oldValue));
					if(deltaValue.equals(oldValue)){
						
//						log.info(this.getClass(), "Minimum deleted");
						
						Map<String, String> res = BytesUtil.convertMapBack(tableService.get(Bytes.toBytes(btu.getViewTable()), Bytes.toBytes(viewRecordKey), BytesUtil.convertList(colFams), new ArrayList<byte[]>(), null).getFamilyMap(Bytes.toBytes(colFams.get(0))));
						res.remove(btu.getKey());
						res.remove(valueName);
//						log.info(this.getClass(), "res: "+res);
						
						Long smallestValue=Long.MAX_VALUE;
					
						for (String bs : res.keySet()) {
							
							try{
								Long val = Long.parseLong(res.get(bs));
								if(val <= smallestValue)smallestValue =val;
							
							}catch(Exception e){
								
							}
							
						}
						if(smallestValue == Long.MAX_VALUE)smallestValue = 0l;
						result = smallestValue;
						
//						log.info(this.getClass(), "new smallest value: "+smallestValue);

						
					}else{
						return null;
					}
				}
					
					
			}
			if(viewMode.equals(ViewMode.AGGREGATION_MAX)){
				
				if(propagationMode.equals(OperationMode.INSERT)){
					newViewRecord.put(Bytes.toBytes(btu.getKey()), Bytes.toBytes(String.valueOf(deltaValue)));
					if(oldValue == 0)oldValue = Long.MIN_VALUE;
	
						if(deltaValue > oldValue)result = deltaValue;
						else result = oldValue;
					
				}
				if(propagationMode.equals(OperationMode.DELETE)){
					
					deleteViewRecord.add(Bytes.toBytes(String.valueOf(btu.getKey())));
					deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, deleteViewRecord, signature);
					deleteViewRecord = new ArrayList<byte[]>();
//					System.out.println("deltaValue: "+deltaValue);
//					System.out.println("oldValue: "+oldValue);
					if(deltaValue.equals(oldValue)){
						
//						log.info(this.getClass(), "Maximum deleted");
						
						Map<String, String> res = BytesUtil.convertMapBack(tableService.get(Bytes.toBytes(btu.getViewTable()), Bytes.toBytes(viewRecordKey), BytesUtil.convertList(colFams), new ArrayList<byte[]>(),null).getFamilyMap(Bytes.toBytes(colFams.get(0))));
						res.remove(btu.getKey());
						res.remove(valueName);
//						log.info(this.getClass(), "res: "+res);
						
						
						Long biggestValue=Long.MIN_VALUE;
						
						for (String bs : res.keySet()) {
							
							try{
								Long val = Long.parseLong(res.get(bs));
								if(val >= biggestValue)biggestValue =val;
							
							}catch(Exception e){
								
							}
							
						}
//						log.info(this.getClass(), "new biggest value: "+biggestValue);
						
						if(biggestValue == Long.MIN_VALUE)biggestValue = 0l;
						result = biggestValue;
						
						
					}else{
						return null;
					}
				}	
			}

			
			
			//UPDATE FOR COUNT,SUM,MIN,MAX

			String checkQualifier = cAV.getAggregationValue();
			String checkValue = null;
			if(oldViewRecord != null){
				checkValue = Bytes.toString(oldViewRecord.get(Bytes.toBytes(cAV.getAggregationValue())));
			}
			
			if(oldViewRecord == null || oldViewRecord.keySet() == null || oldViewRecord.keySet().size() == 0){
				newViewRecord.put( Bytes.toBytes(valueName+"_old") , null);
				newViewRecord.put(Bytes.toBytes(valueName+"_new") , Bytes.toBytes(String.valueOf(result)));
			} else {
				newViewRecord.put( Bytes.toBytes(valueName+"_old") , Bytes.toBytes(String.valueOf(oldValue)));
				newViewRecord.put( Bytes.toBytes(valueName+"_new") , Bytes.toBytes(String.valueOf(result)));
			}
			
			succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), checkQualifier, checkValue, newViewRecord, signature);
			
//			if(result != 0){
//				
//				newViewRecord.put(Bytes.toBytes(valueName), Bytes.toBytes(String.valueOf(result)));
//				log.updates(this.getClass(), viewTableName);
//				log.updates(this.getClass(), viewRecordKey);
//				log.updates(this.getClass(), colFams.get(0));
//				log.updates(this.getClass(), checkQualifier);
//				log.updates(this.getClass(), checkValue);
//				log.updates(this.getClass(), newViewRecord.toString());
//				log.updates(this.getClass(), signature);
//				succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), checkQualifier, checkValue, newViewRecord, signature);
//			}else{
//				
//				deleteViewRecord.add(Bytes.toBytes(valueName));
//				succeed = deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), checkQualifier, checkValue, deleteViewRecord, signature);
//			}
			
		}
		
		
		if(viewMode.equals(ViewMode.SELECTION)){
			
			CreateSelectionView cSV = CreateSelectionView.parse(btu.getViewDefinition());
			log.updates(this.getClass(), "csv: "+cSV);
			String valueName = cSV.getSelectionColumn();
			String operand = cSV.getSelectionOperation();
			Integer selectValue = Integer.parseInt(cSV.getSelectionValue());
			
			// start 
			Map<byte[], byte[]> oldViewRecord=null;
			if(oldVM != null)oldViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(0)));
	
			if(propagationMode.equals(OperationMode.INSERT)){
				
				
				if(oldViewRecord == null || oldViewRecord.keySet() == null || oldViewRecord.keySet().size() == 0){
					
					// Do nothing if both old value and new value are empty.
					if (columns == null || columns.keySet() == null || columns.values().isEmpty()) {
						return true;
					}
					for (String key : columns.keySet()) {				
						newViewRecord.put( Bytes.toBytes(key+"_old") , null);
						
						if (columns.get(valueName) != null && !columns.get(valueName).isEmpty()) {
							Integer value= Integer.parseInt(columns.get(valueName));
					//		log.info(this.getClass(), "base table update selection: valueName="+valueName+", operand="+operand+", conditionalValue="+selectValue+", value="+value);
							
							
							
							boolean isMatching=false;
							
							switch(operand){
							case ">" : isMatching = (value > selectValue);
								break;
							case "<" : isMatching = (value < selectValue);
								break;
							case "=" : isMatching = (value.equals(selectValue));
								break;
							
							
							}
							if(!isMatching){
								// Do nothing if both old value and new value are empty.
								return true;
							} else {
								newViewRecord.put(Bytes.toBytes(key+"_new") , Bytes.toBytes(columns.get(key)));
							}
						}
					}
					
				}else{
					
					for (byte[] key : oldViewRecord.keySet()) {				
						newViewRecord.put( Bytes.toBytes(Bytes.toString(key).replace("_new", "")+"_old") , oldViewRecord.get(key));
					}
					
					for (String key : columns.keySet()) {
						if (columns.get(valueName) != null && !columns.get(valueName).isEmpty()) {
							Integer value= Integer.parseInt(columns.get(valueName));
					//		log.info(this.getClass(), "base table update selection: valueName="+valueName+", operand="+operand+", conditionalValue="+selectValue+", value="+value);
							
							
							
							boolean isMatching=false;
							
							switch(operand){
							case ">" : isMatching = (value > selectValue);
								break;
							case "<" : isMatching = (value < selectValue);
								break;
							case "=" : isMatching = (value.equals(selectValue));
								break;
							
							
							}
							if(!isMatching){
								newViewRecord.put(Bytes.toBytes(key+"_new") , null);
							} else {
								newViewRecord.put(Bytes.toBytes(key+"_new") , Bytes.toBytes(columns.get(key)));
							}
						}
						
					}
					
					
				}
				
				
			}	
			if(propagationMode.equals(OperationMode.DELETE)){
				
				for (byte[] key : oldViewRecord.keySet()) {				
					newViewRecord.put( Bytes.toBytes(Bytes.toString(key).replace("_new", "")+"_old") , oldViewRecord.get(key));
					newViewRecord.put( Bytes.toBytes(Bytes.toString(key)) , null);
				}
				

			}
//			log.info(this.getClass(), "viewRecordKey: "+viewRecordKey);
//			log.info(this.getClass(), "newViewRecord: "+BytesUtil.mapToString(newViewRecord));
			succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, newViewRecord, signature);

		}

		
		if(viewMode.equals(ViewMode.DELTA)){
			
			CreateDeltaView cDV = CreateDeltaView.parse(btu.getViewDefinition());
//			log.info(this.getClass(), "cDV: "+cDV);
			Map<byte[], byte[]> oldViewRecord=null;
			if(oldVM != null)oldViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(0)));
	
			if(propagationMode.equals(OperationMode.INSERT)){
				
				
				if(oldViewRecord == null || oldViewRecord.keySet() == null || oldViewRecord.keySet().size() == 0){
					
					
					for (String key : columns.keySet()) {				
						newViewRecord.put( Bytes.toBytes(key+"_old") , null);
						newViewRecord.put( Bytes.toBytes(key+"_new") , Bytes.toBytes(columns.get(key)));
					}
					
				}else{
					
					for (byte[] key : oldViewRecord.keySet()) {				
						newViewRecord.put( Bytes.toBytes(Bytes.toString(key).replace("_new", "")+"_old") , oldViewRecord.get(key));
					}
					
					for (String key : columns.keySet()) {
						newViewRecord.put( Bytes.toBytes(key+"_new") , Bytes.toBytes(columns.get(key)));
					}
					
					
				}
				
				
			}	
			if(propagationMode.equals(OperationMode.DELETE)){
				
				for (byte[] key : oldViewRecord.keySet()) {				
					newViewRecord.put( Bytes.toBytes(Bytes.toString(key).replace("_new", "")+"_old") , oldViewRecord.get(key));
					newViewRecord.put( Bytes.toBytes(Bytes.toString(key)) , null);
				}
				

			}
//			log.info(this.getClass(), "viewRecordKey: "+viewRecordKey);
//			log.info(this.getClass(), "newViewRecord: "+BytesUtil.mapToString(newViewRecord));
			succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, newViewRecord, signature);
		}
		
		if(viewMode.equals(ViewMode.INDEX)){
			
			
			if(propagationMode.equals(OperationMode.INSERT)){
				
				newViewRecord.put(Bytes.toBytes(String.valueOf(btu.getKey())), Bytes.toBytes(""));
				succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, newViewRecord, signature);
			}	
			if(propagationMode.equals(OperationMode.DELETE)){
				
				deleteViewRecord.add(Bytes.toBytes(String.valueOf(btu.getKey())));
				succeed = deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, deleteViewRecord, signature);
			}
		}
		
		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
			
			log.info(this.getClass(), "to be joined: "+viewRecordKey);
			if (viewRecordKey == null || viewRecordKey == "") {
				return true;
			}
			
			Put put = new Put(Bytes.toBytes(viewRecordKey));
			
			String valueString;
			
			if(propagationMode.equals(OperationMode.INSERT)){
				// If validation fails, retrieve records from view again and update again.
				while (true) {
					// Column family includes current update, which contains columns to be updated.
					String colFam = btu.getBaseTable() + "fam1";
					
					// Join partner from another family, which used to build and update join.
					Map<byte[], byte[]> partnerViewRecord=null;
					Map<byte[], byte[]> count=null;
					Map<byte[], byte[]> joinRecord=null;
					// Will be validate before final result
					int countNumber = 0;
					String joinFam = "joinfam1";
					
					// Store all base table primary keys of partners.
					List<String> partnerKeys = new ArrayList<String>();
					
					// "colFams" consists of partner families and count family, oldVM stores records from partner family
					// and count to validate concurrent update, instead of old columns from delta view. (l1_d1_o: value)
					List<byte[]> getColumns = new ArrayList<byte[]>();
					getColumns = getViewRecordColumns(btu);
					oldVM = retrieveViewRecord(btu, viewRecordKey, BytesUtil.convertList(colFams), getColumns, Bytes.toBytes(signature));
					if(oldVM != null) {
						partnerViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(0)));
						count = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(1)));
						if (count != null && !count.isEmpty()) {
							for (byte[] ks : count.keySet()) {
								countNumber = Bytes.toInt(count.get(ks));
							}
						}
					}
					
					// Build join record with composite key as part of column name, and column name is 
					// combination of composite key + column names from partner + old/new.
					if (partnerViewRecord != null && !partnerViewRecord.isEmpty()) {
						for (byte[] bs : partnerViewRecord.keySet()) {
							log.info(this.getClass(), "kkk: "+ Bytes.toString(bs));
							log.info(this.getClass(), "kkk: "+ Bytes.toString(partnerViewRecord.get(bs)));
							String partnerPK = Bytes.toString(bs).split("_")[0];
							if (!partnerKeys.contains(partnerPK)) {
								partnerKeys.add(partnerPK);
							}
							
							// Update join columns related to primary key in update.
							String compositeColumn;
							// Composite key should be keep in order.
							if (btu.getKey().contains("k")) {
								compositeColumn = btu.getKey() + Bytes.toString(bs);
							} else {
								compositeColumn = Bytes.toString(bs).replace(partnerPK, partnerPK + btu.getKey());
							}
							// Set all join value to null.
							if (containsNullValues(columns) && compositeColumn.contains("_new")) {
								put.add(Bytes.toBytes(joinFam), Bytes.toBytes(compositeColumn), null);
							} else {
								put.add(Bytes.toBytes(joinFam), Bytes.toBytes(compositeColumn), partnerViewRecord.get(bs));
							}
							
							// Copy new to old, only if join family contains corresponding composite
							// key and is not empty. If not, the join pair is just created and value 
							// shouldn't be copied.
							joinRecord = oldVM.getFamilyMap(Bytes.toBytes(joinFam));
							if (Bytes.toString(bs).contains("_new") && 
//								partnerViewRecord.get(bs) != null && 
//								!Bytes.toString(partnerViewRecord.get(bs)).equals("") &&
								joinRecord != null && 
								!joinRecord.isEmpty() && 
								joinRecord.keySet().contains(Bytes.toBytes(compositeColumn))) 
							{
								String oldKey = Bytes.toString(bs).replace("_new", "_old");
								partnerViewRecord.put(Bytes.toBytes(oldKey), partnerViewRecord.get(bs));
								put.add(Bytes.toBytes(colFams.get(0)), Bytes.toBytes(oldKey) , partnerViewRecord.get(bs));
							}
						}
					}
					
					// Get old columns from btu.
					Map<String, String> oldRecordColumns=null;
					oldRecordColumns = btu.getOldColumns(); 
					
					// Update both reverse join columns and join columns related to update. For join
					// columns, column name is combination of composite key + column names from self
					// old/new.
					if(oldRecordColumns != null && !oldRecordColumns.isEmpty() && !containsNullValues(oldRecordColumns)){
						log.info(this.getClass(), "oldRecordColumns: " + oldRecordColumns);
						for (String column : oldRecordColumns.keySet()) {
		//					log.info(this.getClass(), "oldJPRecord"+Bytes.toString(oldViewRecord.get(bs))+"");
							String columnName = btu.getKey() + "_" + column + "_old";
							String oldValue = oldRecordColumns.get(column);
							// Corresponding join column.
							if (!partnerKeys.isEmpty()) {
								for (String partnerPK: partnerKeys) {
									String compositeColumn;
									// Composite key should be keep in order.
									if (partnerPK.contains("k")) {
										compositeColumn = partnerPK + columnName;
									} else {
										compositeColumn = btu.getKey() + partnerPK + "_" + column + "_old";
									}
									
									put.add(Bytes.toBytes(joinFam), Bytes.toBytes(compositeColumn) , Bytes.toBytes(oldValue));
									
								}
								put.add(Bytes.toBytes(colFam), Bytes.toBytes(columnName) , Bytes.toBytes(oldValue));
							} else {
								// If there is no join partners, old value should always be null to avoid
								// error in next sum view.
								put.add(Bytes.toBytes(colFam), Bytes.toBytes(columnName) , null);
							}
						}
						
						for(String colKey : btu.getColumns().keySet()){
							valueString = btu.getColumns().get(colKey);
							
							String columnNameNew = btu.getKey() + "_" + colKey + "_new";
							
							byte[] value;
							if (valueString != null) {
								value = Bytes.toBytes(valueString);
							} else {
								value = null;
							}
							put.add(Bytes.toBytes(colFam), Bytes.toBytes(columnNameNew), value);
							// Corresponding join column.
							if (!partnerKeys.isEmpty()) {
								
								for (String partnerPK: partnerKeys) {
									String compositeColumn;
									if (partnerPK.contains("k")) {
										compositeColumn = partnerPK + columnNameNew;
									} else {
										compositeColumn = btu.getKey() + partnerPK + "_" + colKey + "_new";
									}
									put.add(Bytes.toBytes(joinFam), Bytes.toBytes(compositeColumn) , value);
								}
							}
						}
						
						
					} else {
						for(String colKey : btu.getColumns().keySet()){
							valueString = btu.getColumns().get(colKey);
							
							String columnNameOld = btu.getKey() + "_" + colKey + "_old";
							String columnNameNew = btu.getKey() + "_" + colKey + "_new";
							put.add(Bytes.toBytes(colFam), Bytes.toBytes(columnNameOld), null);
							put.add(Bytes.toBytes(colFam), Bytes.toBytes(columnNameNew), Bytes.toBytes(valueString));
							
							// Corresponding join column.
							if (!partnerKeys.isEmpty()) {
								for (String partnerPK: partnerKeys) {
									String compositeColumnOld;
									String compositeColumnNew;
									if (partnerPK.contains("k")) {
										compositeColumnOld = partnerPK + columnNameOld;
										compositeColumnNew = partnerPK + columnNameNew;
									} else {
										compositeColumnOld = btu.getKey() + partnerPK + "_" + colKey + "_old";
										compositeColumnNew = btu.getKey() + partnerPK + "_" + colKey + "_new";
									}
									put.add(Bytes.toBytes(joinFam), Bytes.toBytes(compositeColumnOld), null);
									put.add(Bytes.toBytes(joinFam), Bytes.toBytes(compositeColumnNew), Bytes.toBytes(valueString));
								}
							}
						}
					}
					
					int validateCount = 0;
					// Validate count number in view again to avoid concurrency issues.
					oldVM = retrieveViewRecord(btu, viewRecordKey, BytesUtil.convertList(colFams), getColumns, Bytes.toBytes(signature));
					if(oldVM != null) {
						partnerViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(0)));
						count = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(1)));
						if (count != null && !count.isEmpty()) {
							for (byte[] ks : count.keySet()) {
								validateCount = Bytes.toInt(count.get(ks));
							}
						}
					}
					
					log.info(this.getClass(), "countNumber: "+countNumber);
					log.info(this.getClass(), "validateCount: "+validateCount);
					if (validateCount == countNumber) {
						put.add(Bytes.toBytes("countfam1"), Bytes.toBytes("count"), Bytes.toBytes(validateCount+1));
						break;
					} else {
						log.info(this.getClass(), "concurrency occurs! ");
					}
				}
				
				succeed = insertToViewTable(viewTableName, viewRecordKey, btu.getBaseTable()+"fam1", null, null, put, signature);
			}	 
			if(propagationMode.equals(OperationMode.DELETE)){
				
				put.add(Bytes.toBytes(btu.getBaseTable()+"fam1"), Bytes.toBytes(btu.getKey()), null);
				succeed = insertToViewTable(viewTableName, viewRecordKey, btu.getBaseTable()+"fam1", null, null, put, signature);
			}					
				

		}
		if(viewMode.equals(ViewMode.JOIN)){
			
			CreateJoinView cJV = CreateJoinView.parse(btu.getViewDefinition());

//			log.updates(this.getClass(), "cJV: "+cJV);
			
//			Map<String, String> cols = removeNullValues(btu.getColumns());
//			log.info(this.getClass(), "cols: "+btu.getColumns());
			Map<String, Map<String, String>> joinKeys = constructJoinKeys(cJV, btu.getColFamilies(), btu.getColumns());
			

			if(propagationMode.equals(OperationMode.INSERT) && btu.getColumns().size() > 1){
				
				
				for (String joinKey : joinKeys.keySet()) {
					
					newViewRecord = BytesUtil.convertMap(joinKeys.get(joinKey));
//					log.info(this.getClass(), "insertJoinKey: "+joinKey);
//					log.info(this.getClass(), "valueMap: "+joinKeys.get(joinKey));
					succeed = insertToViewTable(viewTableName, joinKey, colFams.get(0), null, null, newViewRecord, signature);
				}
				
				
			}
			if(propagationMode.equals(OperationMode.DELETE) && btu.getColumns().size() > 1){
				
			
				
				for (String joinKey : joinKeys.keySet()) {
					
					newViewRecord = BytesUtil.convertMap(columns);
//					log.info(this.getClass(), "deleteJoinKey: "+joinKey);
//					log.info(this.getClass(), "cols: "+joinKeys.get(joinKey).keySet());
					for (String key : joinKeys.get(joinKey).keySet()) {
						
						deleteViewRecord.add(Bytes.toBytes(key));
					}
//					log.info(this.getClass(), "colList: "+BytesUtil.listToString(deleteViewRecord));
					succeed = deleteFromViewTable(viewTableName, joinKey, colFams.get(0), null, null, deleteViewRecord, signature);
					deleteViewRecord = new ArrayList<byte[]>();
				}
				
				
			}
			
			
			
			return true;
		}
			

		return succeed;
	}
	
	

	public boolean containsNullValues(Map<String, String> map){
		
		for (String key : map.keySet()) {
			
			if(map.get(key) == null || map.get(key).equals("") || map.get(key).equals(" ") || map.get(key).equals("null"))return true;
		}
		
		return false;
		
	}
	
	public Map<String, String> removeNullValues(Map<String, String> map){
		
		Map<String, String> result = new HashMap<String, String>();
		
		for (String key : map.keySet()) {
			
			if(map.get(key) != null  && !map.get(key).equals("") && !map.get(key).equals(" "))result.put(key, map.get(key));
		}
		
		return result;
		
	}

	
	public Map<String, Map<String, String>> constructJoinKeys(CreateJoinView cJV, Map<String, String> colFams, Map<String, String> cols){
		
		
		Map<String, Map<String, String>> resultKeys = new HashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> tempKeys = new HashMap<String, Map<String, String>>();
		
		for (String keyPart : cJV.getCompositeKey()) {
			
			for (String colKey : cols.keySet()) {
				
				if(keyPart.equals(colFams.get(colKey))){
					
					if(resultKeys.size() == 0){
						  
						Map<String, String> valueMap = new HashMap<String, String>();
						valueMap.put(colKey, cols.get(colKey));
						tempKeys.put(colKey, valueMap);
						  
					}else{
						for (String resultKey : resultKeys.keySet()) {
							Map<String, String> valueMap = new HashMap<String, String>();
							valueMap.put(colKey, cols.get(colKey));	
							valueMap.putAll(resultKeys.get(resultKey));
							tempKeys.put(resultKey+="_"+colKey, valueMap);
						}
					}
				}
				
			}
			resultKeys = new HashMap<String, Map<String,String>>();
			if(tempKeys.size() == 0)return new HashMap<String, Map<String,String>>();
			resultKeys.putAll(tempKeys);
			tempKeys = new HashMap<String, Map<String,String>>();
			
		}
		return resultKeys;
	}
	
	
	private boolean insertToViewTable(String viewTableName, String key, String columnFamily, String checkQualifier, String checkValue, Map<byte[], byte[]> newViewRecord, String signature) {
		
		Put put = new Put(Bytes.toBytes(key));
		
		for (byte[] columnQualifier : newViewRecord.keySet()) {
			byte[] value = newViewRecord.get(columnQualifier);
			put.add(Bytes.toBytes(columnFamily), columnQualifier, value);	
		}
		return insertToViewTable(viewTableName, key, columnFamily, checkQualifier, checkValue, put,signature);
	}



	private boolean insertToViewTable(String viewTableName, String key, String columnFamily, String checkQualifier, String checkValue, Put put, String signature) {
		
		
		
		if(!SystemConfig.FAULTTOLERANCE_SIGNATURES)signature=null;
		log.updates(this.getClass(), "inserting into view table :"+viewTableName+", key: "+key+", signature: "+signature+", checkQualifier:"+checkQualifier+"checkValue:"+checkValue+" columns: "+put.toString());

		boolean succeed;
		if(SystemConfig.FAULTTOLERANCE_TESTANDSET && checkQualifier != null){
			   
			byte[] checkVal=null;
			if(checkValue != null)checkVal = Bytes.toBytes(checkValue);
			
			succeed = tableService.checkAndPut(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), Bytes.toBytes(checkQualifier), checkVal, put,  (signature != null)?Bytes.toBytes(signature):null);

		}
		else{
			tableService.put(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), put, (signature != null)?Bytes.toBytes(signature):null);
			succeed = true;
		}
		return succeed;
	}
	
	private boolean deleteFromViewTable(String viewTableName, String key, String columnFamily, String checkQualifier, String checkValue, List<byte[]> deleteViewRecord, String signature) {
		
		log.updates(this.getClass(), "deleting from view table :"+viewTableName+", key: "+key+", signature: "+signature+", checkQualifier:"+checkQualifier+"checkValue:"+checkValue+" columns: "+BytesUtil.listToString(deleteViewRecord));
		
		// nothing to delete, when join propagates updates to next aggregation view, this might happen.
		if (key == "" || key == null) {
			return true;
		}
		
		boolean succeed=false;
		if(SystemConfig.FAULTTOLERANCE_TESTANDSET && checkQualifier != null){
			   
			byte[] checkVal=null;
			if(checkValue != null)checkVal = Bytes.toBytes(checkValue);
			
			
			if(SystemConfig.FAULTTOLERANCE_SIGNATURES)
				succeed = tableService.checkAndDeleteWithSignature(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), Bytes.toBytes(checkQualifier), checkVal, deleteViewRecord, Bytes.toBytes(signature));
			else{
				succeed = tableService.checkAndDelete(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), Bytes.toBytes(checkQualifier), checkVal, deleteViewRecord);
			}

		}
		else{
			
			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
				tableService.deleteWithSignature(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), deleteViewRecord, Bytes.toBytes(signature));
				succeed = true;
			}else{
				tableService.delete(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), deleteViewRecord);
				succeed = true;
			}
		}
		return succeed;
	}
	

	
	
	

	
	





	public ICommitLog getCommitLog() {
		return commitLog;
	}

	public void setCommitLog(ICommitLog commitLog) {
		this.commitLog = commitLog;
	}

	public AtomicLong getViewRecordUpdates() {
		return viewRecordUpdates;
	}

	public void setViewRecordUpdates(AtomicLong viewRecordUpdates) {
		this.viewRecordUpdates = viewRecordUpdates;
	}

	public AtomicLong getCommitLogUpdates() {
		return commitLogUpdates;
	}

	public void setCommitLogUpdates(AtomicLong commitLogUpdates) {
		this.commitLogUpdates = commitLogUpdates;
	}

	public ConcurrentHashMap<String, String> getMarkers() {
		return markers;
	}

	public void setMarkers(ConcurrentHashMap<String, String> markers) {
		this.markers = markers;
	}

	
	



	
	
	
	
	
}
