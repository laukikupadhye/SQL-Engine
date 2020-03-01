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
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableSchema {

    private String tableName = "";
    private List<ColumnDefinition> colDefList =  new ArrayList<>();
    private List<String> colList =  new ArrayList<>();
    private Map<String, Integer> colPosition = new HashMap<>();
    private Map<String, ColumnDefinition> colDefMap = new HashMap<>();

    public TableSchema(String tableName, List<ColumnDefinition> colDefList) {
        this.tableName = tableName;
        this.colDefList = colDefList;
        int pos = 0;
        for (ColumnDefinition colDef : colDefList) {
            String colName = colDef.getColumnName();
            colList.add(colName);
            colPosition.put(colName, pos);
            colDefMap.put(colName,colDef);
            pos++;
        }
    }

    public List<String> getListofColumns() {
        return colList;
    }
    
    public int getColPosition(String colName) {
        return colPosition.get(colName);
    }
    
    public ColumnDefinition getColumnDefinition(String colName) { 
        return colDefMap.get(colName);
    }

}
