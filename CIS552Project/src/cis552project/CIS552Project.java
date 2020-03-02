/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class CIS552Project {

	static String dataPath = null;
	static String commandsLoc = null;
	static Map<String, TableSchema> tables = new HashMap<>();

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) {
		commandsLoc = args[0];
		dataPath = args[1];

		try {
			List<String> commands = readCommands(commandsLoc);
			for (String command : commands) {
				Reader reader = new StringReader(command);
				CCJSqlParser parser = new CCJSqlParser(reader);
				try {
					Statement statement = parser.Statement();
					if (statement instanceof Select) {
						Select select = (Select) statement;
						SelectBody selectBody = select.getSelectBody();
						List<String[]> resultArray = selectEvaluation(selectBody);

						printResult(resultArray);
					} else if (statement instanceof CreateTable) {
						createTable(statement);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("Commands location was not identified. Please see the below exception.");
			System.out.println("Exception : " + e.getLocalizedMessage());
			e.printStackTrace();
		}

	}

	private static List<String[]> selectEvaluation(SelectBody selectBody) throws FileNotFoundException {

		List<String[]> pvalResult = new ArrayList<>();
		if (selectBody instanceof PlainSelect) {
			PlainSelect plainSelect = (PlainSelect) selectBody;
			try {
				pvalResult.addAll(evaluateResult(plainSelect));

			} catch (SQLException e) {
				System.out.println("Error Occured while parsing the statements.");
				System.out.println("Statement : " + plainSelect.toString());
				e.printStackTrace();
			}
		}
		return pvalResult;
	}

	private static List<String> readCommands(String filePath) throws FileNotFoundException {
		List<String> commandsList = new ArrayList<>();
		try (Scanner myReader = new Scanner(new File(filePath))) {
			String previousString = "";
			while (myReader.hasNext()) {
				String statement = myReader.next();
				if (!statement.endsWith(";")) {
					previousString += " " + statement;
					continue;
				} else {
					statement = previousString + " " + statement;
					previousString = "";
				}
				commandsList.add(statement);
			}
		}
		return commandsList;
	}

	private static void createTable(Statement statement) {
		CreateTable createTable = (CreateTable) statement;
		String tableName = createTable.getTable().getName();
		TableSchema tableScehma = new TableSchema(tableName, createTable.getColumnDefinitions());
		tables.put(tableName, tableScehma);
	}

	private static List<String[]> evaluateResult(PlainSelect plainSelect) throws FileNotFoundException, SQLException {
		Map<String, String> aliasandTableName = new HashMap<>();
		Map<String, Integer> colPosWithTableAlias = new HashMap<>();
		List<Join> joins = plainSelect.getJoins();
		FromItem fromItem = plainSelect.getFromItem();
		List<FromItem> fromItemList = new ArrayList<>();
		Expression where = plainSelect.getWhere();
		String tableName = fromItem.toString();
		String aliasName = tableName;
		List<String[]> tempResult = new ArrayList<>();
		if (fromItem instanceof SubSelect) {
			SelectBody subSelectBody = ((SubSelect) fromItem).getSelectBody();
			tempResult = selectEvaluation(subSelectBody);
			aliasName = fromItem.getAlias();
			tableName = fromItem.getAlias();
			List<FromItem> fromItems = new ArrayList<>();
			List<SelectItem> selectItems = ((PlainSelect) subSelectBody).getSelectItems();

			fromItems.add(((PlainSelect) subSelectBody).getFromItem());
			if (((PlainSelect) subSelectBody).getJoins() != null) {
				((PlainSelect) subSelectBody).getJoins().forEach(x -> {
					fromItems.add(x.getRightItem());
				});
			}
			copyTableSchemaForAlias(selectItems, fromItems, aliasName, aliasandTableName);
		}
		if (fromItem instanceof Table) {
			Table table = (Table) fromItem;
			tableName = table.getName();
			aliasName = table.getAlias();
			tempResult = CIS552ProjectUtils.readTable(dataPath + "\\" + tableName + ".dat");
			if (aliasName == null) {
				aliasName = tableName;
			}
		}
		addColPosWithTabAlias(tableName, aliasName, colPosWithTableAlias);
		aliasandTableName.put(aliasName, tableName);
		if (joins != null) {
			for (Join join : joins) {

				Table joinTable = null;
				if (join.getRightItem() instanceof Table) {
					joinTable = (Table) join.getRightItem();
				}
				addColPosWithTabAlias(joinTable.getName(), joinTable.getAlias(), colPosWithTableAlias);
				aliasandTableName.put(joinTable.getAlias(), joinTable.getName());
				List<String[]> tempJoinResult = CIS552ProjectUtils
						.readTable(dataPath + "\\" + joinTable.getName() + ".dat");
				List<String[]> tempJoined = new ArrayList<>();
				for (String[] fromRes : tempResult) {
					for (String[] joinRes : tempJoinResult) {
						String[] joined = CIS552ProjectUtils.combineArrays(fromRes, joinRes);
						tempJoined.add(joined);
					}
				}
				tempResult = tempJoined;
			}

		}
		List<String[]> finalResult = new ArrayList<>();
		if (where != null) {
			for (String[] eachRow : tempResult) {
				PrimitiveValue primValue = applyCondition(eachRow, where, fromItemList, aliasandTableName,
						colPosWithTableAlias);
				if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
					finalResult.add(eachRow);
				}
			}

		} else {
			finalResult = tempResult;
		}

		List<FromItem> fromItems = new ArrayList<>();
		List<SelectItem> selectItems = plainSelect.getSelectItems();

		fromItems.add(plainSelect.getFromItem());
		if (plainSelect.getJoins() != null) {
			plainSelect.getJoins().forEach(x -> {
				fromItems.add(x.getRightItem());
			});
		}
		List<String[]> pvalResult = solveSelectItemExpression(finalResult, selectItems, fromItems, aliasandTableName,
				colPosWithTableAlias);
		return pvalResult;
	}

	private static void copyTableSchemaForAlias(List<SelectItem> selectItems, List<FromItem> fromItems,
			String tableName, Map<String, String> aliasandTableName) {
		List<ColumnDefinition> colDefList = new ArrayList<>();
		for (SelectItem si : selectItems) {
			SelectExpressionItem sei = (SelectExpressionItem) si;
			String columnAlias = sei.getAlias();
			Expression exp = sei.getExpression();
			ColumnDefinition colDef = getColDefOfExpression(exp, fromItems);

			if (columnAlias != null) {
				colDef.setColumnName(columnAlias);
			}
			colDefList.add(colDef);
			TableSchema tableSchema = new TableSchema(tableName, colDefList);
			tables.put(tableName, tableSchema);
		}

	}

	private static ColumnDefinition getColDefOfExpression(Expression exp, List<FromItem> fromItems) {

		ColumnDefinition colDef = null;
		if (exp instanceof Column) {
			Column column = (Column) exp;
			// column =
			// determineColumnNameFromWholeName(column.getColumnName(),aliasandTableName);
			colDef = getColDefFromFromItems(column, fromItems);
		}
		if (exp instanceof Addition) {
			Addition add = (Addition) exp;
			colDef = getColDefOfExpression(add.getLeftExpression(), fromItems);
			if (colDef == null) {
				colDef = getColDefOfExpression(add.getRightExpression(), fromItems);
			}
		}
		if (exp instanceof Subtraction) {
			Subtraction sub = (Subtraction) exp;
			colDef = getColDefOfExpression(sub.getLeftExpression(), fromItems);
			if (colDef == null) {
				colDef = getColDefOfExpression(sub.getRightExpression(), fromItems);
			}
		}
		if (exp instanceof Multiplication) {
			Multiplication mul = (Multiplication) exp;
			colDef = getColDefOfExpression(mul.getLeftExpression(), fromItems);
			if (colDef == null) {
				colDef = getColDefOfExpression(mul.getRightExpression(), fromItems);
			}
		}
		if (exp instanceof Division) {
			Division div = (Division) exp;
			colDef = getColDefOfExpression(div.getLeftExpression(), fromItems);
			if (colDef == null) {
				colDef = getColDefOfExpression(div.getRightExpression(), fromItems);
			}
		}
		return colDef;
	}

	private static void addColPosWithTabAlias(String tableName, String aliasName,
			Map<String, Integer> colPosWithTableAlias) {
		TableSchema selectTableTemp = tables.get(tableName);
		List<Column> columnList = selectTableTemp.getListofColumns();
		int colPos = colPosWithTableAlias.size();
		for (Column col : columnList) {
			String colTableMap = aliasName + "." + col.getColumnName();
			colPosWithTableAlias.put(colTableMap, colPos);
			colPos++;
		}
	}

	private static List<String[]> solveSelectItemExpression(List<String[]> rowsResult, List<SelectItem> selectItems,
			List<FromItem> fromItems, Map<String, String> aliasandTableName, Map<String, Integer> colPosWithTableAlias)
			throws SQLException {
		List<String[]> finalResult = new ArrayList<>();
		if (selectItems.get(0) instanceof AllColumns) {
			return rowsResult;
		}
		List<Expression> finalExpItemList = new ArrayList<>();
		for (SelectItem selectItem : selectItems) {
			if (selectItem instanceof SelectExpressionItem) {
				finalExpItemList.add(((SelectExpressionItem) selectItem).getExpression());
			} else if (selectItem instanceof AllTableColumns) {
				AllTableColumns allTableColumn = (AllTableColumns) selectItem;
				TableSchema tableSchema = tables.get(aliasandTableName.get(allTableColumn.getTable().getName()));

				finalExpItemList.addAll(tableSchema.getListofColumns().stream()
						.map(col -> (Expression) new Column(allTableColumn.getTable(), col.getColumnName()))
						.collect(Collectors.toList()));
			}
		}
		for (String[] result : rowsResult) {
			String[] primValToString = new String[finalExpItemList.size()];
			for (int i = 0; i < finalExpItemList.size(); i++) {

				PrimitiveValue value = applyCondition(result, finalExpItemList.get(i), fromItems, aliasandTableName,
						colPosWithTableAlias);
				primValToString[i] = value.toRawString();

			}
			finalResult.add(primValToString);
		}
		return finalResult;
	}

	private static void printResult(List<String[]> rowsResult) throws SQLException {
		System.out.println("Result : ");
		for (String[] result : rowsResult) {
			for (int i = 0; i < result.length; i++) {
				System.out.print(result[i]);
				if (i < result.length - 1) {
					System.out.print("|");
				}
			}
			System.out.println("");
		}
		System.out.println("=");
	}

	private static PrimitiveValue applyCondition(String[] rowResult, Expression where, List<FromItem> fromItems,
			Map<String, String> aliasandTableName, Map<String, Integer> colPosWithTableAlias) throws SQLException {
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column column) throws SQLException {
				String aliasTableName = column.getTable().getName();
				String wholeColumnName = column.getWholeColumnName();
				ColumnDefinition colDef = null;
				if (aliasTableName == null) {
					colDef = getColDefFromFromItems(column, fromItems);
				} else {
					String tableName = aliasandTableName.get(aliasTableName);
					System.out.println("column -- " + column);
					colDef = tables.get(tableName).getColumnDefinition(column.getColumnName());
				}
				SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());

				int pos = colPosWithTableAlias.get(wholeColumnName);
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
		return eval.eval(where);

	}

	protected static ColumnDefinition getColDefFromFromItems(Column column, List<FromItem> fromItems) {
		ColumnDefinition colDef = null;
		for (FromItem fromItem : fromItems) {
			if (fromItem instanceof Table) {
				Table table = (Table) fromItem;
				TableSchema tableSchema = tables.get(table.getName());
				colDef = tableSchema.getColumnDefinition(column.getColumnName());
			}
		}
		return colDef;
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
