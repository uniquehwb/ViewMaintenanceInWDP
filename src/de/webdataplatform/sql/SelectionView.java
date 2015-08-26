package de.webdataplatform.sql;

import java.util.List;

public class SelectionView implements Table {
	private List<Table> baseTables;
	private Table controlTable;
	private String name;
	private String type = "selection";
	private String expression;
	// operator in selection
	private String firstAttr;
	// value to compare in selection
	private String secondAttr;
	
	public SelectionView(String name) {
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
	
	public Table getControlTable() {
		return controlTable;
	}

	public void setControlTable(Table controlTable) {
		this.controlTable = controlTable;
	}
	
	public String getFirstAttr() {
		return firstAttr;
	}

	public void setFirstAttr(String firstAttr) {
		this.firstAttr = firstAttr;
	}

	public String getSecondAttr() {
		return secondAttr;
	}

	public void setSecondAttr(String secondAttr) {
		this.secondAttr = secondAttr;
	}
}
