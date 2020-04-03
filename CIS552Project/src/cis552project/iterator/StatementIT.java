package iterator;

import cis552project.CIS552SO;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class StatementIT extends BaseIT {

	BaseIT result = null;

	public StatementIT(Statement statement, CIS552SO cis552so) {
		
	}

	@Override
	public TableResult getNext() {
		return result.getNext();
	}

	@Override
	public boolean hasNext() {
		if(result == null||result.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

}
