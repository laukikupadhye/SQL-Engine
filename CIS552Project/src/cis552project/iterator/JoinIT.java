package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;

public class JoinIT extends BaseIT {

	BaseIT result1 = null;
	BaseIT result2 = null;
	private TableResult tableResult1 = null;
	private TableResult newTableResult = null;
	Map<Tuple, Expression> joinsExpPushedDown;
	Expression joiningTablesExpressions = null;
	CIS552SO cis552SO;

	public JoinIT(BaseIT result1, BaseIT result2, Map<Tuple, Expression> joinsExpPushedDown, CIS552SO cis552SO) {
		this.result1 = result1;
		this.result2 = result2;
		this.joinsExpPushedDown = joinsExpPushedDown;
		this.cis552SO = cis552SO;
	}

	@Override
	public TableResult getNext() {
		return newTableResult;
	}

	@Override
	public boolean hasNext() {

		if (result1 == null || result2 == null) {
			return false;
		}

//		if (tableResult1 == null && result1.hasNext()) {
//			tableResult1 = result1.getNext();
//		}

//		if (!result2.hasNext()) {
//			result2.reset();
//			if (!result1.hasNext()) {
//				return false;
//			}
//			tableResult1 = result1.getNext();
//		}
		boolean result2HasNext = result2.hasNext();
		while (result2HasNext || result1.hasNext()) {
			if (tableResult1 == null && result1.hasNext()) {
				tableResult1 = result1.getNext();
			}
			if (!result2HasNext) {
				result2.reset();
				tableResult1 = result1.getNext();
				result2HasNext = result2.hasNext();
			}
			TableResult tableResult2 = result2.getNext();
			if (newTableResult == null) {
				newTableResult = new TableResult();
				newTableResult.fromTables.addAll(tableResult1.fromTables);
				newTableResult.fromTables.addAll(tableResult2.fromTables);
				for (Table leftTable : tableResult1.fromTables) {
					for (Table rightTable : tableResult2.fromTables) {
						String leftTableAlias = leftTable.getAlias() != null ? leftTable.getAlias()
								: leftTable.getName();
						String rightTableAlias = rightTable.getAlias() != null ? rightTable.getAlias()
								: rightTable.getName();
						List<String> list = Arrays.asList(leftTableAlias, rightTableAlias);
						Collections.sort(list);

						PrimitiveValue[] joiningTables = { new StringValue(list.get(0)), new StringValue(list.get(1)) };
						Tuple joiningtuple = new Tuple(joiningTables);
						Expression joiningExpression = joinsExpPushedDown.get(joiningtuple);
						if (joiningExpression != null) {
							if (joiningTablesExpressions == null) {
								joiningTablesExpressions = joinsExpPushedDown.get(joiningtuple);
							} else {
								joiningTablesExpressions = new AndExpression(joiningTablesExpressions,
										joinsExpPushedDown.get(joiningtuple));
							}
						}

					}
				}
				newTableResult.colPosWithTableAlias.putAll(tableResult1.colPosWithTableAlias);
				int colPos = newTableResult.colPosWithTableAlias.size();
				for (Entry<Column, Integer> entrySet : tableResult2.colPosWithTableAlias.entrySet()) {
					newTableResult.colPosWithTableAlias.put(entrySet.getKey(), colPos + entrySet.getValue());
				}
				newTableResult.aliasandTableName.putAll(tableResult1.aliasandTableName);
				newTableResult.aliasandTableName.putAll(tableResult2.aliasandTableName);
			}
			newTableResult.resultTuples = new ArrayList<>();
			for (Tuple table1ResultTuple : tableResult1.resultTuples) {
				for (Tuple table2ResultTuple : tableResult2.resultTuples) {
					int length = table1ResultTuple.resultRow.length + table2ResultTuple.resultRow.length;
					PrimitiveValue[] result = new PrimitiveValue[length];
					System.arraycopy(table1ResultTuple.resultRow, 0, result, 0, table1ResultTuple.resultRow.length);
					System.arraycopy(table2ResultTuple.resultRow, 0, result, table1ResultTuple.resultRow.length,
							table2ResultTuple.resultRow.length);
					Tuple tuple = new Tuple(result);
					if (joiningTablesExpressions != null) {
						try {
							Eval eval = new ExpressionEvaluator(Arrays.asList(tuple), newTableResult, cis552SO, null);
							if (joiningTablesExpressions == null)
								System.out.println(tuple);
							PrimitiveValue primValue = eval.eval(joiningTablesExpressions);
							if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
								newTableResult.resultTuples.add(tuple);
								return true;
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}

					} else {
						newTableResult.resultTuples.add(tuple);
						return true;
					}
				}
			}
			result2HasNext = result2.hasNext();
		}
//		TableResult tableResult2 = result2.getNext();
//		List<Expression> joiningTablesExpressions = new ArrayList<>();
//		if (newTableResult == null) {
//			newTableResult = new TableResult();
//			newTableResult.fromTables.addAll(tableResult1.fromTables);
//			newTableResult.fromTables.addAll(tableResult2.fromTables);
//			for (Table leftTable : tableResult1.fromTables) {
//				for (Table rightTable : tableResult2.fromTables) {
//					PrimitiveValue[] joiningTables = { new StringValue(leftTable.getName()),
//							new StringValue(rightTable.getName()) };
//					Tuple joiningtuple = new Tuple(joiningTables);
//					joiningTablesExpressions.add(joinsExpPushedDown.get(joiningtuple));
//				}
//			}
//			newTableResult.colPosWithTableAlias.putAll(tableResult1.colPosWithTableAlias);
//			int colPos = newTableResult.colPosWithTableAlias.size();
//			for (Entry<Column, Integer> entrySet : tableResult2.colPosWithTableAlias.entrySet()) {
//				newTableResult.colPosWithTableAlias.put(entrySet.getKey(), colPos + entrySet.getValue());
//			}
//			newTableResult.aliasandTableName.putAll(tableResult1.aliasandTableName);
//			newTableResult.aliasandTableName.putAll(tableResult2.aliasandTableName);
//		}
//		newTableResult.resultTuples = new ArrayList<>();
//		for (Tuple table1ResultTuple : tableResult1.resultTuples) {
//			for (Tuple table2ResultTuple : tableResult2.resultTuples) {
//				int length = table1ResultTuple.resultRow.length + table2ResultTuple.resultRow.length;
//				PrimitiveValue[] result = new PrimitiveValue[length];
//				System.arraycopy(table1ResultTuple.resultRow, 0, result, 0, table1ResultTuple.resultRow.length);
//				System.arraycopy(table2ResultTuple.resultRow, 0, result, table1ResultTuple.resultRow.length,
//						table2ResultTuple.resultRow.length);
//				newTableResult.resultTuples.add(new Tuple(result));
//			}
//		}
//		return CollectionUtils.isNotEmpty(newTableResult.resultTuples);
		return false;
	}

	@Override
	public void reset() {
		result1.reset();
		result2.reset();
	}

}
