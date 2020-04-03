package cis552project.iterator;

import cis552project.CIS552SO;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromItemIT extends BaseIT {

	BaseIT result = null;

	public FromItemIT(FromItem fromItem, CIS552SO cis552SO) {
		if (fromItem instanceof Table) {
			result = new TableIT((Table) fromItem, cis552SO);
		}

		if (fromItem instanceof SubSelect) {
			result = new SubSelectIT((SubSelect) fromItem, cis552SO);
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
