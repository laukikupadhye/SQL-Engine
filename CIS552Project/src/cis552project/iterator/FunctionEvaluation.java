package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import cis552project.CIS552SO;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class FunctionEvaluation {

	public static long applyFunction(List<Tuple> initialResult, Function funExp, TableResult finalTableResult,
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
			return evaluateMin(initialResult, funExp, expressionList, finalTableResult, cis552so);
		case "MAX":
			return evaluateMax(initialResult, funExp, expressionList, finalTableResult, cis552so);
		}

		throw new UnsupportedOperationException(funName + " Not supported yet.");
	}

	private static long evaluateCount(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		if (funExp.isAllColumns()) {
			return initialResult.size();
		} else {
			List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
					expressionList.getExpressions().get(0), finalTableResult, cis552so);

			return pValueList.size();

		}
	}

	private static long evaluateSum(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		long sum = 0;
		for (PrimitiveValue primitiveValue : pValueList) {
			sum += ((LongValue) primitiveValue).getValue();
		}
		return sum;
	}

	private static long evaluateAvg(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		long sum = 0;
		for (PrimitiveValue primitiveValue : pValueList) {
			sum += ((LongValue) primitiveValue).getValue();
		}
		return sum / pValueList.size();
	}

	private static long evaluateMin(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		long min = ((LongValue) pValueList.get(0)).getValue();
		for (PrimitiveValue primitiveValue : pValueList) {
			if (min <= ((LongValue) primitiveValue).getValue())
				continue;
			min = ((LongValue) primitiveValue).getValue();
		}
		return min;
	}

	private static long evaluateMax(List<Tuple> initialResult, Function funExp, ExpressionList expressionList,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = fetchEvaluatedExpressions(initialResult,
				expressionList.getExpressions().get(0), finalTableResult, cis552so);
		long max = ((LongValue) pValueList.get(0)).getValue();
		for (PrimitiveValue primitiveValue : pValueList) {
			if (max >= ((LongValue) primitiveValue).getValue())
				continue;
			max = ((LongValue) primitiveValue).getValue();
		}
		return max;
	}

	private static List<PrimitiveValue> fetchEvaluatedExpressions(List<Tuple> initialResult, Expression expression,
			TableResult finalTableResult, CIS552SO cis552so) throws SQLException {
		List<PrimitiveValue> pValueList = new ArrayList<>();
		for (Tuple tuple : initialResult) {
			PrimitiveValue value = ExpressionEvaluator.applyCondition(tuple.resultRow, expression, finalTableResult,
					cis552so);
			pValueList.add(value);
		}

		return pValueList;
	}

}
