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
	static Map<String, String> aliasandTableName = new HashMap<>();
	static Map<String, Integer> colPosWithTableAlias = new HashMap<>();

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
						// printResult(finalResult, selectItems);
					} else if (statement instanceof CreateTable) {
						createTable(statement);
					}
				} catch (ParseException e) {
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
		List<String[]> finalResult = new ArrayList<>();
		List<SelectItem> selectItems = new ArrayList<>();
		if (selectBody instanceof PlainSelect) {
			PlainSelect plainSelect = (PlainSelect) selectBody;
			try {
				finalResult = evaluateResult(plainSelect);

				selectItems = plainSelect.getSelectItems();

			} catch (SQLException e) {
				System.out.println("Error Occured while parsing the statements.");
				System.out.println("Statement : " + plainSelect.toString());
				e.printStackTrace();
			}
		}
		Object[] result = { finalResult, selectItems };
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

	private static List<String[]> evaluateResult(PlainSelect plainSelect) throws FileNotFoundException, SQLException {
		List<Join> joins = plainSelect.getJoins();
		FromItem fromItem = plainSelect.getFromItem();

		Expression where = plainSelect.getWhere();
		String tableName = fromItem.toString();
		String aliasName = tableName;
		
		if (fromItem instanceof SubSelect) {
			Object[] objectResult = selectEvaluation(((SubSelect) fromItem).getSelectBody());
			List<String[]> finalResult = (List<String[]>) objectResult[0];
			List<SelectItem> selectItems = (List<SelectItem>) objectResult[1];
		}
		if (fromItem instanceof Table) {
			if (fromItem.getAlias() != null) {
				if (fromItem instanceof Table) {
					tableName = fetchTableNameFromALias(fromItem);
				}
			}
		}
		System.out.println("plainSelect - " + plainSelect);
		
		addColPosWithTabAlias(tableName, aliasName, colPosWithTableAlias.size());
		aliasandTableName.put(aliasName, tableName);

		List<String[]> tempResult = CIS552ProjectUtils.readTable(dataPath + "\\" + tableName + ".dat");
		if (joins != null) {
			for (Join join : joins) {
				String joinTableName = fromItem.toString();
				String joinAliasName = tableName;

				if (join.getRightItem().getAlias() != null) {
					joinAliasName = join.getRightItem().getAlias();
					joinTableName = fetchTableNameFromALias(join.getRightItem());
				}
				addColPosWithTabAlias(joinTableName, joinAliasName, colPosWithTableAlias.size());
				aliasandTableName.put(joinAliasName, joinTableName);
				List<String[]> tempJoinResult = CIS552ProjectUtils.readTable(dataPath + "\\" + joinTableName + ".dat");
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
				PrimitiveValue primValue = applyCondition(eachRow, where);
				if (primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()) {
					finalResult.add(eachRow);
				}
			}

		} else {
			finalResult = tempResult;
		}
		return finalResult;
	}

	private static void addColPosWithTabAlias(String tableName, String aliasName, Integer pos) {
		TableSchema selectTableTemp = tables.get(tableName);
		List<String> colNm = selectTableTemp.getListofColumns();
		for (String s : colNm) {
			String colTableMap = aliasName + "." + s;
			colPosWithTableAlias.put(colTableMap, pos);
			pos++;
		}
	}

	private static void printResult(List<String[]> rowsResult, List<SelectItem> selectItems) throws SQLException {
		for (String[] result : rowsResult) {
			for (SelectItem selectItem : selectItems) {
				System.out.print(applyCondition(result, ((SelectExpressionItem) selectItem).getExpression()));
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

	private static PrimitiveValue applyCondition(String[] rowResult, Expression where) throws SQLException {
		Eval eval = new Eval() {
			@Override
			public PrimitiveValue eval(Column column) throws SQLException {
				System.out.println("(column.getTable() + \".\" + column.getColumnName())"
						+ (column.getTable() + "." + column.getColumnName()));
				int pos = colPosWithTableAlias.get((column.getTable() + "." + column.getColumnName()));
				String value = rowResult[pos];
				System.out.println("column.getTable().getName() - " + column.getTable().getName());
				String tableName = aliasandTableName.get(column.getTable().getName());
				SQLDataType colSqlDataType = tables.get(tableName).getSQLDataType(column.getColumnName());
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
		return eval.eval(expressionResolver(where));

	}

	private static Expression expressionResolver(Expression exp) {
		if (exp instanceof InExpression) {

			if (((InExpression) exp).getItemsList() instanceof SubSelect) {
				expressionResolver(new InExpression(((InExpression) exp).getLeftExpression(),
						((InExpression) exp).getItemsList()));
			}
		}
		return exp;
	}

}
