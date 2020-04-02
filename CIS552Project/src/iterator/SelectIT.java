package iterator;

import cis552project.CIS552SO;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class SelectIT extends BaseIT {

	BaseIT result = null;

	public SelectIT(Select select, CIS552SO cis552so) {
		if (select.getSelectBody() instanceof PlainSelect) {
			result = new PlainSelectIT((PlainSelect) select.getSelectBody(), cis552so);
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
		// TODO Auto-generated method stub

	}

}
