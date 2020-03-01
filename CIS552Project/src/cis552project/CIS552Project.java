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
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
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
						Object[] resultArray = selectEvaluation(selectBody);
						List<String[]> finalResult = (List<String[]>) resultArray[0];
						List<SelectItem> selectItems = (List<SelectItem>) resultArray[1];
						List<FromItem> fromItems = (List<FromItem>) resultArray[2];
						Map<String, String> aliasandTableName = (Map<String, String>) resultArray[3];
						Map<String, Integer> colPosWithTableAlias = (Map<String, Integer>) resultArray[4];
						printResult(finalResult, selectItems, fromItems, aliasandTableName, colPosWithTableAlias);
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

	private static Object[] selectEvaluation(SelectBody selectBody) throws FileNotFoundException {

		Map<String, String> aliasandTableName = new HashMap<>();
		Map<String, Integer> colPosWithTableAlias = new HashMap<>();
		List<String[]> finalResult = new ArrayList<>();
		List<SelectItem> selectItems = new ArrayList<>();
		List<FromItem> fromItems = new ArrayList<>();
		if (selectBody instanceof PlainSelect) {
			PlainSelect plainSelect = (PlainSelect) selectBody;
			try {
				finalResult = evaluateResult(plainSelect, aliasandTableName, colPosWithTableAlias);

				selectItems = plainSelect.getSelectItems();

				fromItems.add(plainSelect.getFromItem());
				if (plainSelect.getJoins() != null) {
					plainSelect.getJoins().forEach(x -> {
						fromItems.add(x.getRightItem());
					});
				}
			} catch (SQLException e) {
				System.out.println("Error Occured while parsing the statements.");
				System.out.println("Statement : " + plainSelect.toString());
				e.printStackTrace();
			}
		}
		Object[] result = { finalResult, selectItems, fromItems, aliasandTableName, colPosWithTableAlias };
		return result;
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

	private static List<String[]> evaluateResult(PlainSelect plainSelect, Map<String, String> aliasandTableName,
			Map<String, Integer> colPosWithTableAlias) throws FileNotFoundException, SQLException {
		List<Join> joins = plainSelect.getJoins();
		FromItem fromItem = plainSelect.getFromItem();
		List<FromItem> fromItemList = new ArrayList<>();
		Expression where = plainSelect.getWhere();
		String tableName = fromItem.toString();
		String aliasName = tableName;
		List<String[]> tempResult = new ArrayList<>();
		if (fromItem instanceof SubSelect) {
			Object[] objectResult = selectEvaluation(((SubSelect) fromItem).getSelectBody());
			tempResult = (List<String[]>) objectResult[0];
			List<SelectItem> selectItems = (List<SelectItem>) objectResult[1];
			List<FromItem> fromItems = (List<FromItem>) objectResult[2];
			aliasName = fromItem.getAlias();
			tableName = fromItem.getAlias();
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
				System.out.println("evaluate - ");
				PrimitiveValue primValue = applyCondition(eachRow, where, fromItemList, aliasandTableName,
						colPosWithTableAlias);
				if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
					finalResult.add(eachRow);
				}
			}

		} else {
			finalResult = tempResult;
		}
		return finalResult;
	}

	private static void copyTableSchemaForAlias(List<SelectItem> selectItems, List<FromItem> fromItems,
			String tableName, Map<String, String> aliasandTableName) {
		System.out.println("selectItems --- ");
		List<ColumnDefinition> colDefList = new ArrayList<>();
		for (SelectItem si : selectItems) {
			SelectExpressionItem sei = (SelectExpressionItem) si;
			String columnAlias = sei.getAlias();
			System.out.println("sei.getClass()--" + sei.getExpression());
			Expression exp = sei.getExpression();
			Column column = null;
			ColumnDefinition colDef = null;
			if (exp instanceof Column) {
				column = (Column) exp;
				// column = determineColumnNameFromWholeName(column.getColumnName(),aliasandTableName);
				colDef = getColDefFromFromItems(column, fromItems);
			} else {
				
			}
			System.out.println();
			if (columnAlias != null) {
				colDef.setColumnName(columnAlias);
			}
			colDefList.add(colDef);
			TableSchema tableSchema = new TableSchema(tableName, colDefList);
			tables.put(tableName, tableSchema);
		}

	}

	private static void addColPosWithTabAlias(String tableName, String aliasName,
			Map<String, Integer> colPosWithTableAlias) {
		TableSchema selectTableTemp = tables.get(tableName);
		List<String> colNm = selectTableTemp.getListofColumns();
		int colPos = colPosWithTableAlias.size();
		for (String s : colNm) {
			String colTableMap = aliasName + "." + s;
			System.out.println("colTableMap*** - " + colTableMap + " -- colPos - " + colPos);
			colPosWithTableAlias.put(colTableMap, colPos);
			colPos++;
		}
	}

	private static void printResult(List<String[]> rowsResult, List<SelectItem> selectItems, List<FromItem> fromItems,
			Map<String, String> aliasandTableName, Map<String, Integer> colPosWithTableAlias) throws SQLException {

		System.out.println("print colPosWithTableAlias");
		colPosWithTableAlias.entrySet().forEach(System.out::println);
		for (String[] result : rowsResult) {
			for (SelectItem selectItem : selectItems) {
				System.out.print(applyCondition(result, ((SelectExpressionItem) selectItem).getExpression(), fromItems,
						aliasandTableName, colPosWithTableAlias));
				if (selectItems.indexOf(selectItem) < selectItems.size() - 1) {
					System.out.print("|");
				}
			}
			System.out.println("");
		}
		System.out.println("=");
	}

	private static String fetchTableNameFromALias(FromItem fromItem) {
		System.out.println("fromItem - " + fromItem);
		String tableName = fromItem.toString().substring(0, fromItem.toString().indexOf(" AS "));
		return tableName;
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
					colDef = tables.get(tableName).getColumnDefinition(column.getColumnName());
				}
				SQLDataType colSqlDataType = SQLDataType.valueOf(colDef.getColDataType().getDataType().toUpperCase());

				int pos = colPosWithTableAlias.get(wholeColumnName);
				System.out.println("colPosWithTableAlias !!! - ");
				colPosWithTableAlias.entrySet().forEach(System.out::println);
				System.out.println("rowResult - " + rowResult);
				for (int i = 0; i < rowResult.length; i++) {
					System.out.println(rowResult[i]);
				}
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
				System.out.println("table - " + table);
				System.out.println("tableSchema - " + tableSchema);
				System.out.println("colDef - " + colDef);
				if (colDef == null) {
					System.out.println("when it is null--" + column.getColumnName());
					continue;
				} else {
					break;
				}
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
