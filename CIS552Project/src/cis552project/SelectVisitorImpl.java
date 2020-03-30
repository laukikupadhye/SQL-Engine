package cis552project;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

public class SelectVisitorImpl implements SelectVisitor {

	private SelectItemVisitor selectItemVisitor = new SelectItemVisitorImpl();
	@Override
	public void visit(PlainSelect arg0) {
		for (SelectItem selectItem : arg0.getSelectItems()) {
			selectItem.accept(selectItemVisitor);
		}
	}

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub

	}

}
