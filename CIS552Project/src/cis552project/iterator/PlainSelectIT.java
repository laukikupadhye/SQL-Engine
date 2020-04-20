package cis552project.iterator;

import java.sql.SQLException;
import java.util.Map;

import cis552project.CIS552SO;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class PlainSelectIT extends BaseIT {

	BaseIT result = null;

	public PlainSelectIT(PlainSelect plainSelect, CIS552SO cis552SO) throws SQLException {
		result = new FromItemIT(plainSelect.getFromItem(), cis552SO);
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				BaseIT result2 = new FromItemIT(join.getRightItem(), cis552SO);
				result = new JoinIT(result, result2);
			}
		}
		if (plainSelect.getWhere() != null) {
			result = new WhereIT(plainSelect.getWhere(), result, cis552SO);
		}
		if (plainSelect.getGroupByColumnReferences() != null) {
			result = new GroupByIT(plainSelect.getGroupByColumnReferences(), result, cis552SO);
		}
		result = new SelectItemIT(plainSelect.getSelectItems(), result, cis552SO);

		if (plainSelect.getDistinct() != null) {
			result = new DistinctIT(result);
		}

		if (plainSelect.getOrderByElements() != null) {
			result = new OrderByIT(plainSelect.getOrderByElements(), result, cis552SO);
		}
		if (plainSelect.getLimit() != null) {
			result = new LimitIT(plainSelect.getLimit(), result);
		}
	}

	public PlainSelectIT(PlainSelect plainSelect, CIS552SO cis552SO, Map<Column, PrimitiveValue> outerQueryColResult)
			throws SQLException {
		result = new FromItemIT(plainSelect.getFromItem(), cis552SO);
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				BaseIT result2 = new FromItemIT(join.getRightItem(), cis552SO);
				result = new JoinIT(result, result2);
			}
		}
		if (plainSelect.getWhere() != null) {
			result = new WhereIT(plainSelect.getWhere(), result, cis552SO, outerQueryColResult);
		}
		if (plainSelect.getGroupByColumnReferences() != null) {
			result = new GroupByIT(plainSelect.getGroupByColumnReferences(), result, cis552SO);
		} else {
			result = new SelectItemIT(plainSelect.getSelectItems(), result, cis552SO);

			if (plainSelect.getDistinct() != null) {
				result = new DistinctIT(result);
			}
		}
		if (plainSelect.getOrderByElements() != null) {
			result = new OrderByIT(plainSelect.getOrderByElements(), result, cis552SO);
		}
		if (plainSelect.getLimit() != null) {
			result = new LimitIT(plainSelect.getLimit(), result);
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
