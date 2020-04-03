package cis552project.iterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import cis552project.CIS552ProjectUtils;
import cis552project.CIS552SO;
import cis552project.TableColumnData;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class TableIT extends BaseIT {
	File tableFile = null;
	TableResult tableRes = null;
	Scanner fileScanner = null;

	public TableIT(Table table, CIS552SO cis552SO) {
		try {
			tableRes = new TableResult();
			tableFile = new File(cis552SO.dataPath, table.getName() + ".dat");
			fileScanner = new Scanner(tableFile);
			TableColumnData selectTableTemp = cis552SO.tables.get(table.getName());
			String aliasName = table.getAlias() != null ? table.getAlias() : table.getName();
			int colPos = 0;
			for (Column col : selectTableTemp.colList) {
				tableRes.colPosWithTableAlias.put(new Column(new Table(aliasName), col.getColumnName()), colPos);
				colPos++;
			}
			tableRes.colDefMap = selectTableTemp.colDefMap;
			tableRes.fromItems.add(table);
			tableRes.aliasandTableName.put(aliasName, table.getName());
		} catch (FileNotFoundException ex1) {
			Logger.getLogger(CIS552ProjectUtils.class.getName()).log(Level.SEVERE, null, ex1);
		}

	}

	@Override
	public TableResult getNext() {
		List<Tuple> resultRows = new ArrayList<>();
		resultRows.add(new Tuple(fileScanner.nextLine().split("\\|")));
		tableRes.resultTuples = resultRows;
		return tableRes;
	}

	@Override
	public boolean hasNext() {
		return fileScanner.hasNext();
	}

	@Override
	public void reset() {
		try {
			fileScanner = new Scanner(tableFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
