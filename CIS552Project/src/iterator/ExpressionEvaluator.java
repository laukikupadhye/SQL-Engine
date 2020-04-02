package iterator;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import cis552project.CIS552SO;
import cis552project.SQLDataType;
import cis552project.TableColumnData;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;

public class ExpressionEvaluator {
	
	public static PrimitiveValue applyCondition(String[] rowResult, Expression where, TableResult tabResult, CIS552SO cis552SO) throws SQLException {
			Eval eval = new Eval() {
				@Override
				public PrimitiveValue eval(Column column) throws SQLException {
					Table table = column.getTable();
					ColumnDefinition colDef = null;
					if (table == null || table.getName() == null) {
						if (getTableSchemaForColumnFromFromItems(column, tabResult.getFromItems(), cis552SO) == null) {
						}
						table = getTableSchemaForColumnFromFromItems(column, tabResult.getFromItems(), cis552SO).getTable();
						column.setTable(table);
					}
					String tableName = tabResult.getAliasandTableName().get(table.getName());
					colDef = cis552SO.tables.get(tableName).getColumnDefinition(column.getColumnName());
					SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());

					int pos = tabResult.getColPosWithTableAlias().get(column.getWholeColumnName());
					String value = rowResult[pos];
					switch (colSqlDataType) {
					case CHAR:
					case VARCHAR:
					case STRING:
						return new StringValue(value);
					case DATE:
						return new DateValue(value);
					case DECIMAL:
						return new DoubleValue(value);
					case INT:
						return new LongValue(value);
					}
					throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods,
					// choose Tools | Templates.
				}

			};
			return eval.eval(expressionEvaluator(where, tabResult, cis552SO));
		
	}
	
	private static Expression expressionEvaluator(Expression exp, TableResult tabResult, CIS552SO cis552so) {
		if (exp instanceof Between) {
			Between bet = (Between) exp;
			if (bet.getLeftExpression() instanceof Column) {

				Column column = (Column) bet.getLeftExpression();
				Table table = column.getTable();
				ColumnDefinition colDef = null;
				if (table == null || table.getName() == null) {
					table = getTableSchemaForColumnFromFromItems(column, tabResult.getFromItems(), cis552so).getTable();
				}
				String tableName = tabResult.getAliasandTableName().get(table.getName());
				colDef = cis552so.tables.get(tableName).getColumnDefinition(column.getColumnName());
				SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());
				if (SQLDataType.DATE.equals(colSqlDataType)) {
					bet.setBetweenExpressionEnd(
							new DateValue(bet.getBetweenExpressionEnd().toString().replace("'", "")));
					bet.setBetweenExpressionStart(
							new DateValue(bet.getBetweenExpressionStart().toString().replace("'", "")));
				}
			}
		}
		return exp;
	}

	protected static TableColumnData getTableSchemaForColumnFromFromItems(Column column, List<FromItem> fromItems, CIS552SO cis552so) {
		for (FromItem fromItem : fromItems) {
			if (fromItem instanceof Table) {
				Table table = (Table) fromItem;
				TableColumnData tableSchema = cis552so.tables.get(table.getName());
				if (tableSchema.containsColumn(column.getColumnName())) {
					return tableSchema;
				}
			}
		}
		return null;
	}

	public static Column determineColumnNameFromWholeName(String columnName, Map<String, String> aliasandTableName) {
		Table table = null;
		if (columnName.contains(".")) {
			String aliasTableName = columnName.substring(0, columnName.indexOf("."));
			columnName = columnName.substring(columnName.indexOf(".") + 1, columnName.length());
			String tableName = aliasandTableName.get(aliasTableName);
			table = new Table(tableName);
			table.setAlias(aliasTableName);
		}
		return new Column(table, columnName);

	}

}