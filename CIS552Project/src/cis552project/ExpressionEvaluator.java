package cis552project;

import java.sql.SQLException;
import java.util.List;

import cis552project.iterator.FunctionEvaluation;
import cis552project.iterator.TableResult;
import cis552project.iterator.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ExpressionEvaluator extends Eval {
	private List<Tuple> resultTuples;
	private TableResult tabResult;
	private CIS552SO cis552SO;

	public ExpressionEvaluator(List<Tuple> resultTuples, TableResult tabResult, CIS552SO cis552SO) throws SQLException {
		this.resultTuples = resultTuples;
		this.tabResult = tabResult;
		this.cis552SO = cis552SO;
	}

	@Override
	public PrimitiveValue eval(Column column) throws SQLException {
		Table table = column.getTable();
		if (table == null || table.getName() == null) {
			table = CIS552ProjectUtils.getTable(column, tabResult.colPosWithTableAlias.keySet(), cis552SO);
			column.setTable(table);
		}

		int pos = tabResult.colPosWithTableAlias.get(column);
		return resultTuples.get(0).resultRow[pos];
	}

	@Override
	public PrimitiveValue eval(Between between) throws SQLException {
		if (between.getLeftExpression() instanceof Column) {

			Column column = (Column) between.getLeftExpression();
			Table table = column.getTable();
			ColumnDefinition colDef = null;
			if (table == null || table.getName() == null) {
				table = CIS552ProjectUtils.getTableSchemaForColumnFromFromItems(column, tabResult.fromItems,
						cis552SO).table;
			}
			String tableName = tabResult.aliasandTableName.get(table.getName());
			colDef = cis552SO.tables.get(tableName).colDefMap.get(column.getColumnName());
			SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());
			if (SQLDataType.DATE.equals(colSqlDataType)) {
				between.setBetweenExpressionEnd(
						new DateValue(between.getBetweenExpressionEnd().toString().replace("'", "")));
				between.setBetweenExpressionStart(
						new DateValue(between.getBetweenExpressionStart().toString().replace("'", "")));
			}
		}
		return super.eval(between);

	}

	@Override
	public PrimitiveValue eval(Function function) throws SQLException {
		return FunctionEvaluation.applyFunction(resultTuples, function, tabResult, cis552SO);
	}

	@Override
	public PrimitiveValue eval(ExistsExpression existExp) throws SQLException {

		return null;
	}
}
