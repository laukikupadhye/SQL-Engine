package cis552project.iterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cis552project.CIS552SO;
import net.sf.jsqlparser.schema.Column;

public class GroupByIT extends BaseIT {

	Map<List<Column>, List<Tuple>> groupByMap = new HashMap<>();

	public GroupByIT(List<Column> list, BaseIT result, CIS552SO cis552so) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public TableResult getNext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

}
