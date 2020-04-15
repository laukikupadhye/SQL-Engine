package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

public class WhereIT extends BaseIT {
	BaseIT result = null;
	Expression where = null;
	CIS552SO cis552SO = null;

	public WhereIT(Expression where, BaseIT result, CIS552SO cis552SO) {
		this.cis552SO = cis552SO;
		this.result = result;
		this.where = where;
	}

	@Override
	public TableResult getNext() {
		TableResult tableResult = result.getNext();
		List<Tuple> finalResult = new ArrayList<>();
		for (Tuple eachTuple : tableResult.resultTuples) {
			try {
				Eval eval = new ExpressionEvaluator(Arrays.asList(eachTuple), tableResult, cis552SO);
				PrimitiveValue primValue = eval.eval(where);
				if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
					finalResult.add(eachTuple);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		tableResult.resultTuples = finalResult;

		return tableResult;
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
