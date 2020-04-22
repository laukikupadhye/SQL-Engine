package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class AggFunctionIT extends BaseIT {

	TableResult finalTableResult = null;
	Iterator<Tuple> resIT = null;
	List<Tuple> finalResultTuples = null;
	CIS552SO cis552SO = null;

	public AggFunctionIT(BaseIT result, List<SelectItem> selectItems, CIS552SO cis552SO) throws SQLException {
		this.cis552SO = cis552SO;
		List<Tuple> resultTuples = new ArrayList<>();

		TableResult initialTabRes = null;
		DoubleValue[] primValToString = new DoubleValue[selectItems.size()];

		while (result.hasNext()) {
			initialTabRes = result.getNext();
			for (int i = 0; i < selectItems.size(); i++) {

				Eval eval = new ExpressionEvaluator(initialTabRes.resultTuples, initialTabRes, cis552SO);
				PrimitiveValue primitivevalue = eval.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
				if (primValToString[i] == null) {
					primValToString[i] = new DoubleValue(0);
				}
				primValToString[i] = new DoubleValue(primValToString[i].toDouble() + primitivevalue.toDouble());
			}
			resultTuples.addAll(initialTabRes.resultTuples);
		}

		finalResultTuples = new ArrayList<>();
		finalResultTuples.add(new Tuple(primValToString));

		resIT = finalResultTuples.iterator();
		finalTableResult = new TableResult();
		updateColDefMap(finalTableResult, selectItems);
	}

	@Override
	public TableResult getNext() {
		finalTableResult.resultTuples = new ArrayList<>();
		finalTableResult.resultTuples.add(resIT.next());
		return finalTableResult;
	}

	@Override
	public boolean hasNext() {
		return resIT.hasNext();
	}

	@Override
	public void reset() {
		resIT = finalResultTuples.iterator();
	}

	private void updateColDefMap(TableResult newTableResult, List<SelectItem> selectItems) {
		int pos = 0;
		for (SelectItem si : selectItems) {
			SelectExpressionItem sei = (SelectExpressionItem) si;
			Expression exp = sei.getExpression();
			String columnName = exp.toString();
			if (exp instanceof Column) {
				Column column = (Column) exp;
				columnName = column.getColumnName();
			}
			String columnAlias = sei.getAlias() != null ? sei.getAlias() : columnName;
			newTableResult.colPosWithTableAlias.put(new Column(null, columnAlias), pos);

		}
		pos++;
	}

}
