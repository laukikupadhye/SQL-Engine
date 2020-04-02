package iterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import cis552project.CIS552ProjectUtils;
import cis552project.CIS552SO;
import cis552project.TableColumnData;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class TableIT extends BaseIT {
	RandomAccessFile raFile = null;
	TableResult tableRes = null;

	public TableIT(Table table, CIS552SO cis552so) {
		try {
			tableRes = new TableResult();
			raFile = new RandomAccessFile(cis552so.dataPath + "/" + table.getName() + ".dat", "r");
			addColPosWithTabAlias(table, cis552so.tables);
			tableRes.getFromItems().add(table);
		} catch (FileNotFoundException ex1) {
			Logger.getLogger(CIS552ProjectUtils.class.getName()).log(Level.SEVERE, null, ex1);
		}

	}

	@Override
	public TableResult getNext() {
		return tableRes;
	}

	@Override
	public boolean hasNext() {
		String nextLine = null;
		try {
			nextLine = raFile.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (nextLine == null) {
			return false;
		}
		List<String[]> resultRows = new ArrayList<>();
		resultRows.add(nextLine.split("\\|"));
		tableRes.resultTuples = resultRows;
		return true;
	}

	@Override
	public void reset() {
		try {
			raFile.seek(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void addColPosWithTabAlias(Table table, Map<String, TableColumnData> tables) {

		TableColumnData selectTableTemp = tables.get(table.getName());
		List<Column> columnList = selectTableTemp.getListofColumns();
		String aliasName = table.getAlias() != null ? table.getAlias() : table.getName();
		int colPos = 0;
		for (Column col : columnList) {
			String colTableMap = aliasName + "." + col.getColumnName();
			tableRes.getColPosWithTableAlias().put(colTableMap, colPos);
			colPos++;
		}

		tableRes.getAliasandTableName().put(aliasName, table.getName());
	}
}
