/*
` * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import iterator.BaseIT;
import iterator.SelectIT;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class CIS552Project {

	public static void main(String[] args) {
		CIS552SO cis552so = new CIS552SO();
		String commandsLoc = args[0];
		cis552so.dataPath = args[1];

		try {
			List<String> commands = CIS552ProjectUtils.readCommands(commandsLoc);
			for (String command : commands) {
				try (Reader reader = new StringReader(command)) {
					CCJSqlParser parser = new CCJSqlParser(reader);
					Statement statement = parser.Statement();

					if (statement instanceof CreateTable) {
						createTable(statement, cis552so);
						System.out.println("Table Create Successfully");
					} else if (statement instanceof Select) {
						BaseIT result = new SelectIT((Select) statement, cis552so);
						while (result.hasNext()) {
							printResult(result.getNext().getResultTuples());
						}
					}
				} catch (ParseException e) {
					System.out.println("Exception : " + e.getLocalizedMessage());
				} finally {
					System.out.println("=");
				}
			}
		} catch (IOException e) {

			System.out.println("Commands location was not identified. Please see the below exception.");
			System.out.println("Exception : " + e.getLocalizedMessage());
		}

	}

	private static void createTable(Statement statement, CIS552SO cis552so) {
		CreateTable createTable = (CreateTable) statement;
		String tableName = createTable.getTable().getName();
		TableColumnData tableColData = new TableColumnData(new Table(tableName), createTable.getColumnDefinitions());
		cis552so.tables.put(tableName, tableColData);
	}

	private static void printResult(List<String[]> rowsResult) {
		for (String[] result : rowsResult) {
			System.out.println(String.join("|", result));
		}
	}
}
