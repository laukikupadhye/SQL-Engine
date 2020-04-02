package iterator;

import cis552project.CIS552SO;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

public class FromItemIT extends BaseIT {

	BaseIT result = null;

	public FromItemIT(FromItem fromItem, CIS552SO cis552so) {
		if (fromItem instanceof Table) {
			result = new TableIT((Table) fromItem, cis552so);
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
