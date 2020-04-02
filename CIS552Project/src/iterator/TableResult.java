package iterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;

import net.sf.jsqlparser.statement.select.FromItem;

public class TableResult {

	Map<String, Integer> colPosWithTableAlias = null;
	List<String[]> resultTuples = null;
	Map<String, String> aliasandTableName = null;
	List<FromItem> fromItems = null;

	public TableResult() {
		colPosWithTableAlias = new HashedMap<String, Integer>();
		resultTuples = new ArrayList<>();
		aliasandTableName = new HashMap<>();
		fromItems = new ArrayList<>();
	}

	public List<String[]> getResultTuples() {
		return resultTuples;
	}

	public Map<String, Integer> getColPosWithTableAlias(){
		return colPosWithTableAlias;
	}

	public Map<String, String> getAliasandTableName() {
		return aliasandTableName;
	}

	public List<FromItem> getFromItems() {
		return fromItems;
	}
}
