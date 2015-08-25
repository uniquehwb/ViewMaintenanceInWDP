package de.webdataplatform.sql;

import java.util.List;

public class ReverseJoinView implements Table{
	
	private List<Table> baseTables;
	private String name;
	private String type = "reversejoin";
	private String expression;
	
	public ReverseJoinView(String name) {
		this.name = name;
	}
	
	public List<Table> getBaseTables() {
		return baseTables;
	}

	public void setBaseTables(List<Table> baseTables) {
		this.baseTables = baseTables;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

}
