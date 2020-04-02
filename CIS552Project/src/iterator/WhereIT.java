package iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import cis552project.CIS552SO;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

public class WhereIT extends BaseIT {
	BaseIT result = null;
	Expression where = null;
	CIS552SO cis552so = null;

	public WhereIT(Expression where, BaseIT result, CIS552SO cis552so) {
		this.cis552so = cis552so;
		this.result = result;
		this.where = where;
	}

	@Override
	public TableResult getNext() {
		TableResult tableResult = result.getNext();
		List<String[]> finalResult = new ArrayList<>();
		for (String[] eachRow : tableResult.getResultTuples()) {
			try {
				PrimitiveValue primValue = ExpressionEvaluator.applyCondition(eachRow, where, tableResult, cis552so);
				if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
					finalResult.add(eachRow);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
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
		// TODO Auto-generated method stub

	}

}
