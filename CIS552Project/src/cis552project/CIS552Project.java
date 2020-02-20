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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import org.apache.commons.lang3.tuple.Pair;

public class CIS552Project {

    static String dataPath = null;
    static String commandsLoc = null;
    static Map<String, Map<String, String>> tables = new HashMap<>();
    static Map<Pair<String,String>, Integer> resultIndexes = new HashMap<>();

    static Map<String, String> sql2JavaType = new HashMap<>();

    {
        sql2JavaType.put("string", "String");
        sql2JavaType.put("varchar", "String");
        sql2JavaType.put("char", "String");
        sql2JavaType.put("int", "Long");
        sql2JavaType.put("decimal", "Double");
        sql2JavaType.put("decimal", "Date");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        dataPath = args[0];
        commandsLoc = args[1];

        try {
            List<String> commands = readCommands(commandsLoc);
            commands.stream().forEach(System.out::println);
            for (String command : commands) {
                Reader reader = new StringReader(command);
                CCJSqlParser parser = new CCJSqlParser(reader);
                Statement statement = parser.Statement();
                if (statement instanceof Select) {
                    Select select = (Select) statement;
                    SelectBody selectBody = select.getSelectBody();
                    if (selectBody instanceof PlainSelect) {
                        PlainSelect plainSelect = (PlainSelect) selectBody;
                        evaluateResult(plainSelect);

                    }
                } else if (statement instanceof CreateTable) {
                    CreateTable createTable = (CreateTable) statement;
                    tables.put(createTable.getTable().getWholeTableName(),
                            createTable.getColumnDefinitions().stream().
                                    collect(Collectors.toMap(x -> x.getColumnName(),
                                            x -> x.getColDataType().getDataType())));
                }
            }
        } catch (ParseException | FileNotFoundException ex) {
            Logger.getLogger(CIS552Project.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static List<String> readCommands(String filePath) throws FileNotFoundException {
        File myObj = new File(filePath);
        List<String> commandsList = new ArrayList<>();
        try ( Scanner myReader = new Scanner(myObj)) {
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

    private static void evaluateResult(PlainSelect plainSelect) throws FileNotFoundException {

        List<SelectItem> selectItems = plainSelect.getSelectItems();
        List<Join> joins = plainSelect.getJoins();
        FromItem fromItem = plainSelect.getFromItem();
        Distinct distinct = plainSelect.getDistinct();
        List<Column> groupByColumnReferences = plainSelect.getGroupByColumnReferences();
        Expression having = plainSelect.getHaving();
        Table into = plainSelect.getInto();
        Limit limit = plainSelect.getLimit();
        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        Top top = plainSelect.getTop();
        Expression where = plainSelect.getWhere();

        // Reuse index logic and find out how to use it globally in joins as well.
        Map<String, String> columnDef = tables.get(fromItem.toString());
        Set<String> keySet = columnDef.keySet();
        List<String> keysList = keySet.stream().collect(Collectors.toList());
        System.out.println(selectItems);
        List<Integer> posn = new ArrayList<>();
        selectItems.forEach((selectItem) -> {
            posn.add(keysList.indexOf(selectItem.toString()));
        });
        
        List<String[]> rowResults = readTable(dataPath + "\\" + fromItem.toString() + ".dat");
        if (where != null) {

        }
        Map<String, String> tableAliasNames = new HashMap<>();
        
        printResult(rowResults);
//        File myObj = new File(dataPath + "\\" + fromItem.toString() + ".dat");
//        try ( Scanner myReader = new Scanner(myObj)) {
//            while (myReader.hasNext()) {
//                String dataRow = myReader.next();
//                String[] split = dataRow.split("\\|");
//                String singleRowResult = "";
//                for (Integer i : posn) {
//                    singleRowResult += "|" + split[i] + "|";
//                }
//                System.out.println(singleRowResult);
//            }
//        }
    }

    private static List<String[]> readTable(String filePath) throws FileNotFoundException {
        File myObj = new File(filePath);
        List<String[]> resultRows = new ArrayList<>();
        try ( Scanner myReader = new Scanner(myObj)) {
            while (myReader.hasNext()) {
                String dataRow = myReader.next();
                resultRows.add(dataRow.split("\\|"));
                Pair<String, String> key = null;
            }
        }
        return resultRows;
    }
    
    private static void printResult(List<String[]> rowsResult){
        System.out.println("Result :");
//        System.out.println(String.join("|", selectItems);
        rowsResult.forEach(result -> {
//            String singleRowResult = "";
//                for (int i=0; i<selectItems.size();i++) {
//                    singleRowResult += "|" + result[i] + "|";
//                }
                System.out.println(String.join("|", result));
        });
        System.out.println("==================================================================================");
    }

}
