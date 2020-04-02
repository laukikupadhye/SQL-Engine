package iterator;

import cis552project.CIS552SO;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class PlainSelectIT extends BaseIT {

	BaseIT result = null;

	public PlainSelectIT(PlainSelect plainSelect, CIS552SO cis552so) {
		result = new FromItemIT(plainSelect.getFromItem(), cis552so);
		if (plainSelect.getWhere() != null) {
			result = new WhereIT(plainSelect.getWhere(), result, cis552so);
		}
		result = new SelectItemIT(plainSelect.getSelectItems(), result, cis552so);
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
