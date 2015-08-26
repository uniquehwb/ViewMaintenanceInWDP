package de.webdataplatform.sql;

import java.util.List;

public interface Table {

	public List<Table> getBaseTables();
	public void setBaseTables(List<Table> baseTables);
	
	public Table getControlTable();
	public void setControlTable(Table controlTable);

	public String getName();
	public void setName(String name);

	public String getType();
	public void setType(String type);
	
	public String getExpression();
	public void setExpression(String expression);
	
	public String getFirstAttr();
	public void setFirstAttr(String firstAttr);
	
	public String getSecondAttr();
	public void setSecondAttr(String secondAttr);
}
