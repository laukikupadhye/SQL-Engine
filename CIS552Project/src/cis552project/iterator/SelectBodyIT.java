package cis552project.iterator;

import cis552project.CIS552SO;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;

public class SelectBodyIT extends BaseIT {

	BaseIT result = null;

	public SelectBodyIT(SelectBody selectBody, CIS552SO cis552SO) {
		if (selectBody instanceof PlainSelect) {
			result = new PlainSelectIT((PlainSelect) selectBody, cis552SO);
		}
	}

	@Override
	public TableResult getNext() {
		return result.getNext();
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

}
