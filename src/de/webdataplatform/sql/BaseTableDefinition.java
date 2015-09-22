package de.webdataplatform.sql;

import java.util.Map;

public class BaseTableDefinition {
	
	private String name;
	private String rowkeyPrefix;
	// Column name (including column family) and corresponding prefix
	private Map<String, String> columns;
	
	public BaseTableDefinition(String name, String rowkeyPrefix, Map<String, String> columns) {
		this.setName(name);
		this.setRowkeyPrefix(rowkeyPrefix);
		this.setColumns(columns);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRowkeyPrefix() {
		return rowkeyPrefix;
	}

	public void setRowkeyPrefix(String rowkeyPrefix) {
		this.rowkeyPrefix = rowkeyPrefix;
	}

	public Map<String, String> getColumns() {
		return columns;
	}

	public void setColumns(Map<String, String> columns) {
		this.columns = columns;
	}
	
	
}
