package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class AggFunctionIT extends BaseIT {

	TableResult finalTableResult = null;
	Iterator<Tuple> resIT = null;
	List<Tuple> finalResultTuples = null;
	CIS552SO cis552SO = null;

//	public AggFunctionIT(Function funExp, BaseIT result, CIS552SO cis552so, String columnAlias) {
//		this.cis552SO = cis552so;
//		List<Tuple> resultTuples = new ArrayList<>();
//		while (result.hasNext()) {
//			TableResult initialTabRes = result.getNext();
//			if (finalTableResult == null) {
//				finalTableResult = new TableResult();
//				finalTableResult.aliasandTableName.putAll(initialTabRes.aliasandTableName);
////				finalTableResult.colDefMap.putAll(initialTabRes.colDefMap);
//				finalTableResult.colPosWithTableAlias.putAll(initialTabRes.colPosWithTableAlias);
//				finalTableResult.fromItems.addAll(initialTabRes.fromItems);
//			}
//			resultTuples.addAll(initialTabRes.resultTuples);
//		}
//		try {
//			PrimitiveValue resultValue = FunctionEvaluation.applyFunction(resultTuples, funExp, finalTableResult,
//					cis552so);
//			finalResultTuples = new ArrayList<>();
//			PrimitiveValue[] pvArray = { resultValue };
//			finalResultTuples.add(new Tuple(pvArray));
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		resIT = finalResultTuples.iterator();
//	}

	public AggFunctionIT(BaseIT result, CIS552SO cis552SO, List<SelectItem> selectItems) throws SQLException {
		this.cis552SO = cis552SO;
		List<Tuple> resultTuples = new ArrayList<>();

		TableResult initialTabRes = null;
		while (result.hasNext()) {
			initialTabRes = result.getNext();
			if (finalTableResult == null) {
				finalTableResult = new TableResult();
				finalTableResult.aliasandTableName.putAll(initialTabRes.aliasandTableName);
//				finalTableResult.colDefMap.putAll(initialTabRes.colDefMap);
				finalTableResult.colPosWithTableAlias.putAll(initialTabRes.colPosWithTableAlias);
				finalTableResult.fromItems.addAll(initialTabRes.fromItems);
			}
			resultTuples.addAll(initialTabRes.resultTuples);
		}
		PrimitiveValue[] primValToString = new PrimitiveValue[selectItems.size()];
		for (int i = 0; i < selectItems.size(); i++) {

			Eval eval = new ExpressionEvaluator(resultTuples, initialTabRes, cis552SO);
			primValToString[i] = eval.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());

		}

		finalResultTuples = new ArrayList<>();
		finalResultTuples.add(new Tuple(primValToString));

		resIT = finalResultTuples.iterator();
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

}
