package cis552project.iterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;

public class TableResult {

	public Map<Column, Integer> colPosWithTableAlias = new HashedMap<>();
	public List<Tuple> resultTuples = new ArrayList<>();
	public Map<String, String> aliasandTableName = new HashMap<>();
	public List<FromItem> fromItems = new ArrayList<>();

	public Map<String, ColumnDefinition> colDefMap = new HashMap<>();

}
