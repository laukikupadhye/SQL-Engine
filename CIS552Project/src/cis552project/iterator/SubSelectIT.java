package cis552project.iterator;

import java.util.Map.Entry;

import cis552project.CIS552SO;
import cis552project.TableColumnData;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SubSelectIT extends BaseIT {

	BaseIT result = null;
	SubSelect subSelect = null;
	CIS552SO cis552SO = null;
	TableResult newTableResult = null;

	public SubSelectIT(SubSelect subSelect, CIS552SO cis552SO) {
		this.subSelect = subSelect;
		this.cis552SO = cis552SO;
		SelectBody subSelectBody = subSelect.getSelectBody();
		result = new SelectBodyIT(subSelectBody, cis552SO);

	}

	@Override
	public TableResult getNext() {
		TableResult oldTableResult = result.getNext();
		if (newTableResult == null) {
			newTableResult = new TableResult();
			copyTableInfoForSubSelect(oldTableResult, newTableResult);
		}
		newTableResult.resultTuples = oldTableResult.resultTuples;
		return newTableResult;
	}

	@Override
	public boolean hasNext() {
		if (result == null || !result.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public void reset() {
		result.reset();
	}

	private void copyTableInfoForSubSelect(TableResult oldTableResult, TableResult newTableResult) {
		String tableName = subSelect.getAlias();
		newTableResult.fromItems.add(new Table(tableName));
		newTableResult.aliasandTableName.put(tableName, tableName);

		TableColumnData tableSchema = new TableColumnData(new Table(tableName), oldTableResult.colDefMap.values());
		for (Entry<Column, Integer> entrySet : oldTableResult.colPosWithTableAlias.entrySet()) {
			Column column = new Column(new Table(tableName), entrySet.getKey().getColumnName());
			newTableResult.colPosWithTableAlias.put(column, entrySet.getValue());
		}
		cis552SO.tables.put(tableName, tableSchema);
	}

}
