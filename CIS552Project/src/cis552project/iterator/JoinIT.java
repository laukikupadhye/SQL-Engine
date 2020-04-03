package cis552project.iterator;

import java.util.ArrayList;
import java.util.Map.Entry;

import net.sf.jsqlparser.schema.Column;

public class JoinIT extends BaseIT {

	BaseIT result1 = null;
	BaseIT result2 = null;
	private TableResult tableResult1 = null;
	private TableResult newTableResult = null;

	public JoinIT(BaseIT result1, BaseIT result2) {
		this.result1 = result1;
		this.result2 = result2;
	}

	@Override
	public TableResult getNext() {
		if (tableResult1 == null) {
			tableResult1 = result1.getNext();
		}
		if (!result2.hasNext()) {
			result2.reset();
			tableResult1 = result1.getNext();
		}
		TableResult tableResult2 = result2.getNext();
		if (newTableResult == null) {
			newTableResult = new TableResult();
			newTableResult.fromItems.addAll(tableResult1.fromItems);
			newTableResult.fromItems.addAll(tableResult2.fromItems);
			newTableResult.colPosWithTableAlias.putAll(tableResult1.colPosWithTableAlias);
			int colPos = newTableResult.colPosWithTableAlias.size();
			for (Entry<Column, Integer> entrySet : tableResult2.colPosWithTableAlias.entrySet()) {
				newTableResult.colPosWithTableAlias.put(entrySet.getKey(), colPos + entrySet.getValue());
			}
			newTableResult.aliasandTableName.putAll(tableResult1.aliasandTableName);
			newTableResult.aliasandTableName.putAll(tableResult2.aliasandTableName);
		}
		newTableResult.resultTuples = new ArrayList<>();
		for (Tuple table1ResultTuple : tableResult1.resultTuples) {
			for (Tuple table2ResultTuple : tableResult2.resultTuples) {
				int length = table1ResultTuple.resultRow.length + table2ResultTuple.resultRow.length;
				String[] result = new String[length];
				System.arraycopy(table1ResultTuple.resultRow, 0, result, 0, table1ResultTuple.resultRow.length);
				System.arraycopy(table2ResultTuple.resultRow, 0, result, table1ResultTuple.resultRow.length,
						table2ResultTuple.resultRow.length);
				newTableResult.resultTuples.add(new Tuple(result));
			}
		}
		return newTableResult;
	}

	@Override
	public boolean hasNext() {

		if (result1 == null || result2 == null || !result1.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public void reset() {
		result1.reset();
		result2.reset();
	}

}
