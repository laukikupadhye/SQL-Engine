/*
` * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import io.TableScan;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
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
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class CIS552Project {

	static String dataPath = null;
	static String commandsLoc = null;
	static Map<String, TableColumnData> tables = new HashMap<>();

	static Map<String, TableColumnData> tempTableDef = new HashMap<>();

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) {
		commandsLoc = args[0];
		dataPath = args[1];

		try {
			List<String> commands = CIS552ProjectUtils.readCommands(commandsLoc);
			for (String command : commands) {
				try (Reader reader = new StringReader(command)) {
					CCJSqlParser parser = new CCJSqlParser(reader);
					Statement statement = parser.Statement();
					if (statement instanceof CreateTable) {
						createTable(statement);
						System.out.println("Table Create Successfully");
					} else if (statement instanceof Select) {
						Select select = (Select) statement;
						SelectBody selectBody = select.getSelectBody();
						selectEvaluation(selectBody);
					}
				} catch (SQLException | ParseException e) {
					System.out.println("Exception : " + e.getLocalizedMessage());
				} finally {
					System.out.println("=");
				}
				tempTableDef = new HashMap<>();
			}
		} catch (IOException e) {

			System.out.println("Commands location was not identified. Please see the below exception.");
			System.out.println("Exception : " + e.getLocalizedMessage());
		}

	}

	private static void createTable(Statement statement) {
		CreateTable createTable = (CreateTable) statement;
		String tableName = createTable.getTable().getName();
		TableColumnData tableColData = new TableColumnData(new Table(tableName), createTable.getColumnDefinitions());
		tempTableDef.put(tableName, tableColData);
	}

	private static List<String[]> selectEvaluation(SelectBody selectBody) throws IOException, SQLException {

		List<String[]> tempResult = new ArrayList<>();
		if (selectBody instanceof Union) {
			Union union = (Union) selectBody;
			for (PlainSelect plainSelect : union.getPlainSelects()) {
				evaluatePlainSelect(plainSelect);
			}
		} else if (selectBody instanceof PlainSelect) {
			PlainSelect plainSelect = (PlainSelect) selectBody;
			evaluatePlainSelect(plainSelect);
		}
		return tempResult;
	}

	private static void evaluatePlainSelect(PlainSelect plainSelect) throws SQLException, IOException {
		FromItem fromItem = plainSelect.getFromItem();
		if(fromItem instanceof Table) {
			Table fromTable = (Table) fromItem;
			TableScan fromTableScan = new TableScan(dataPath, fromTable.getName());
			
		}
	}
}
