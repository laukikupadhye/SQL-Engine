/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableSchema {

    private Table table = new Table();
    private List<Column> colList =  new ArrayList<>();
    private Map<String, Integer> colPosition = new HashMap<>();
    private Map<String, ColumnDefinition> colDefMap = new HashMap<>();

    public TableSchema(Table table, List<ColumnDefinition> colDefList) {
        this.table = table;
        int pos = 0;
        for (ColumnDefinition colDef : colDefList) {
        	String columnName = colDef.getColumnName();
        	Column column = new Column(table, columnName);
            colList.add(column);
            colPosition.put(columnName, pos);
            colDefMap.put(columnName,colDef);
            pos++;
        }
    }

    public List<Column> getListofColumns() {
        return colList;
    }
    
    public int getColPosition(String colName) {
        return colPosition.get(colName);
    }
    
    public ColumnDefinition getColumnDefinition(String colName) { 
        return colDefMap.get(colName);
    }

    public boolean containsColumn(String columnName) {
    	for (Column col : colList) {
			if(col.getColumnName().equals(columnName)) {
				return true;
			}
		}
    	return false;
    }

	public Table getTable() {
		return table;
	}
}
