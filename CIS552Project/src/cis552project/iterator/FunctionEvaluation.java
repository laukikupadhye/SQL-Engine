package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class FunctionEvaluation {

	public static PrimitiveValue applyFunction(List<Tuple> initialResult, Function funExp, TableResult finalTableResult,
			CIS552SO cis552so) throws SQLException {
		String funName = funExp.getName().toUpperCase();
		ExpressionList expressionList = funExp.getParameters();
		switch (funName) {
		case "COUNT":
			return evaluateCount(initialResult, funExp, expressionList, finalTableResult, cis552so);
		case "SUM":
			return evaluateSum(initialResult, funExp, expressionList, finalTableResult, cis552so);
		case "AVG":
			return evaluateAvg(initialResult, funExp, expressionList, finalTableResult, cis552so);
		case "MIN":
			PrimitiveValue test = evaluateMin(initialResult, funExp, expressionList, finalTableResult, cis552so);
			return test;
		case "MAX":
			return evaluateMax(initialResult, funExp, expressionList, finalTableResult, cis552so);
		}

		throw new UnsupportedOperationException(funName + " Not supported yet.");
	}

	private static PrimitiveValue evaluateCount(List<Tuple> initialResult, Function funExp,
			ExpressionList expressionList, TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		if (funExp.isAllColumns()) {
			return new LongValue(initialResult.size());
		} else {
			List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
					expressionList.getExpressions().get(0), finalTableResult, cis552so);

			return new LongValue(pValueList.size());

		}
	}

	private static PrimitiveValue evaluateSum(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		Double sum = 0.0;
		for (PrimitiveValue primitiveValue : pValueList) {
			sum += ((LongValue) primitiveValue).getValue();
		}
		return new DoubleValue(sum);
	}

	private static PrimitiveValue evaluateAvg(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		double sum = 0;
		double avg = 0;
		for (PrimitiveValue primitiveValue : pValueList) {
			sum += ((LongValue) primitiveValue).getValue();
		}
		avg = sum / pValueList.size();
		PrimitiveValue returnVal = new DoubleValue(avg);
		return returnVal;
	}

	private static PrimitiveValue evaluateMin(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		double min = pValueList.get(0).toDouble();
		for (PrimitiveValue primitiveValue : pValueList) {
			if (min <= primitiveValue.toDouble())
				continue;
			min = primitiveValue.toDouble();
		}
		return new DoubleValue(min);
	}

	private static PrimitiveValue evaluateMax(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		double max = pValueList.get(0).toDouble();
		for (PrimitiveValue primitiveValue : pValueList) {
			if (max >= primitiveValue.toDouble())
				continue;
			max = primitiveValue.toDouble();
		}
		return new DoubleValue(max);
	}

	private static List<PrimitiveValue> fetchEvaluatedExpressions(List<Tuple> initialResult, Expression expression,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = new ArrayList<>();
		for (Tuple tuple : initialResult) {
			Eval eval = new ExpressionEvaluator(Arrays.asList(tuple), finalTableResult, cis552so);
			PrimitiveValue value = eval.eval(expression);
			pValueList.add(value);
		}

		return pValueList;
	}

}
