package de.webdataplatform.sql;

import java.util.List;

public interface Table {

	public List<Table> getBaseTables();
	public void setBaseTables(List<Table> baseTables);

	public String getName();
	public void setName(String name);

	public String getType();
	public void setType(String type);
	
	public String getExpression();
	public void setExpression(String expression);
}
