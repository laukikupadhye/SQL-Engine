package cis552project.iterator;

import java.sql.SQLException;
import java.util.Map;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

public class WhereIT extends BaseIT {
	BaseIT result = null;
	Expression where = null;
	CIS552SO cis552SO = null;
	Map<Column, PrimitiveValue> outerQueryColResult = null;
	TableResult tableResult;

//	public WhereIT(Expression where, BaseIT result, CIS552SO cis552SO) {
//		this.cis552SO = cis552SO;
//		this.result = result;
//		this.where = where;
//	}

	public WhereIT(Expression where, BaseIT result, CIS552SO cis552SO,
			Map<Column, PrimitiveValue> outerQueryColResult) {
		this.cis552SO = cis552SO;
		this.result = result;
		this.where = where;
		this.outerQueryColResult = outerQueryColResult;
	}

	@Override
	public TableResult getNext() {
		return tableResult;
	}

	@Override
	public boolean hasNext() {
		if (result == null) {
			return false;
		}

		while (result.hasNext()) {
			tableResult = result.getNext();
			try {
				Eval eval = new ExpressionEvaluator(tableResult.resultTuples, tableResult, cis552SO,
						outerQueryColResult);
				PrimitiveValue primValue = eval.eval(where);
				if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
					return true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	@Override
	public void reset() {
		result.reset();
	}

}
