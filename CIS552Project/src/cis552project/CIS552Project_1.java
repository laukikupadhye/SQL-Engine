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
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
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

public class CIS552Project_1 {

    static String dataPath = null;
    static String commandsLoc = null;
    static Map<String, List<String>> tables = new HashMap<>();
    static Map<String, Integer> colPositions = new HashMap<>();
    static Map<String, ColDataType> colTypes = new HashMap<>();
    static Map<String, String> aliasandTableName = new HashMap<>();

//    static Map<String, String> sql2JavaType = new HashMap<>();
//
//    {
//        sql2JavaType.put("string", "String");
//        sql2JavaType.put("varchar", "String");
//        sql2JavaType.put("char", "String");
//        sql2JavaType.put("int", "Long");
//        sql2JavaType.put("decimal", "Double");
//        sql2JavaType.put("decimal", "Date");
//    }
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
                    createTable(statement);
                }
            }
        } catch (ParseException | FileNotFoundException ex) {
            Logger.getLogger(CIS552Project_1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static List<String> readCommands(String filePath) throws FileNotFoundException {
        List<String> commandsList = new ArrayList<>();
        try ( Scanner myReader = new Scanner(new File(filePath))) {
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
        int index = 0;

        String tableName = createTable.getTable().getWholeTableName();
        List<String> colNamesList = new ArrayList<>();
        for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
            String colName = columnDefinition.getColumnName();
            colNamesList.add(colName);
            colPositions.put(tableName + "." + colName, index);
            colTypes.put(tableName + "." + colName, columnDefinition.getColDataType());
            index++;
        }
        tables.put(tableName, colNamesList);
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

        String tableName = fromItem.toString();
        String aliasName = tableName;
        if (fromItem.getAlias() != null) {
            aliasName = fromItem.getAlias();
            tableName = fetchTableNameFromALias(fromItem);
        }
        aliasandTableName.put(aliasName, tableName);

        // Reuse index logic and find out how to use it globally in joins as well.
        System.out.println("____" + fromItem.getAlias() + "_____" + fromItem.toString());
        List<String[]> rowResults = readTable(dataPath + "\\" + tableName + ".dat");
        if (where != null) {
            applyCondition(rowResults, where);
        }
        printResult(rowResults, selectItems);
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

    private static void printResult(List<String[]> rowsResult, List<SelectItem> selectItems) {

        System.out.println("Result :");
        String[] finalREsult = new String[selectItems.size()];
        Integer[] pos = new Integer[selectItems.size()];
        for (int i = 0; i < selectItems.size(); i++) {
            String selectALiasName = selectItems.get(i).toString().substring(0, selectItems.get(i).toString().indexOf("."));
            String selectColumnName = selectItems.get(i).toString().substring(selectItems.get(i).toString().indexOf(".") + 1, selectItems.get(i).toString().length());
            String tableNAme = aliasandTableName.get(selectALiasName);
            pos[i] = colPositions.get(tableNAme + "." + selectColumnName);
        }
        rowsResult.forEach(result -> {
            for (int i = 0; i < pos.length; i++) {
                finalREsult[i] = result[pos[i]];
            }

//            String singleRowResult = "";
//                for (int i=0; i<selectItems.size();i++) {
//                    singleRowResult += "|" + result[i] + "|";
//                }
            System.out.println(String.join("|", finalREsult));
        });
        System.out.println("==================================================================================");
    }

    private static String fetchTableNameFromALias(FromItem fromItem) {
        String tableName = fromItem.toString().substring(0, fromItem.toString().indexOf(" AS "));
        return tableName;
    }

    private static void applyCondition(List<String[]> rowResults, Expression where) {
        functionExpression(where);
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {
                column.getTable();
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
//        PrimitiveValue result;
//        if(where instanceof GreaterThan){
//            GreaterThan gtExp = (GreaterThan) where;
//            result = eval.eval(new GreaterThan(where, where));
//        };

    }

    private static Expression functionExpression(Expression exp) {
        if (exp instanceof Column) {
            
            System.out.println(((Column) exp).getTable() + "----"+((Column) exp).getColumnName());
            return new Column(((Column) exp).getTable(), ((Column) exp).getColumnName());
        }

        if (exp instanceof DateValue) {
            return ((DateValue) exp);
        }
        if (exp instanceof DoubleValue) {
            return ((DoubleValue) exp);
        }
        if (exp instanceof Function) {
            return ((Function) exp);
        }
        if (exp instanceof NullValue) {
            return ((NullValue) exp);
        }
        if (exp instanceof LongValue) {
            return ((LongValue) exp);
        }
        if (exp instanceof StringValue) {
            System.out.println("!!!!!!!!sadasdas" + ((StringValue) exp).getValue());
            return ((StringValue) exp);
        }
        if (exp instanceof TimestampValue) {
            return ((TimestampValue) exp);
        }
        if (exp instanceof TimeValue) {
            return ((TimeValue) exp);
        }

        if (exp instanceof GreaterThan) {
            GreaterThan e = (GreaterThan) exp;
            Expression rightExpression = e.getRightExpression();
                System.out.println("!!!!!");
            functionExpression(e.getLeftExpression());
        }
        if (exp instanceof Addition) {
            Addition e = (Addition) exp;
            System.out.println(e);
        }
        if (exp instanceof OrExpression) {
            OrExpression e = (OrExpression) exp;
            if (e.getRightExpression() instanceof GreaterThan) {
                functionExpression(e.getRightExpression());
            }
            System.out.println(e);
        }
        return null;
    }

}