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
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.jfr.Experimental;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
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
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
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
    static Map<String, TableSchema> tables = new HashMap<>();
    static Map<String, String> aliasandTableName = new HashMap<>();
    static Map<String, Integer> colPosWithTableAlias = new HashMap<>();

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
        } catch (ParseException | FileNotFoundException | SQLException ex) {
            Logger.getLogger(CIS552Project.class.getName()).log(Level.SEVERE, null, ex);
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
        String tableName = createTable.getTable().getName();
        TableSchema tableScehma = new TableSchema(tableName, createTable.getColumnDefinitions());
        tables.put(tableName, tableScehma);
    }

    private static void evaluateResult(PlainSelect plainSelect) throws FileNotFoundException, SQLException {
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
        System.out.println("Select Items: " + selectItems);
        String tableName = fromItem.toString();
        String aliasName = tableName;

        if (fromItem.getAlias() != null) {
            aliasName = fromItem.getAlias();
            tableName = fetchTableNameFromALias(fromItem);
        }
        addColPosWithTabAlias(tableName, aliasName, colPosWithTableAlias.size());
        //System.out.println("map size:"+colPosWithTableAlias.size());

        //colPosWithTableAlias.put((aliasName+"."+col).toString(), Integer.MIN_VALUE)
        aliasandTableName.put(aliasName, tableName);
        //int restPos= 0;

        List<String[]> tempResult = readTable(dataPath + "\\" + tableName + ".dat");

        // Reuse index logic and find out how to use it globally in joins as well.
        System.out.println("____" + fromItem.getAlias() + "_____" + fromItem.toString());
        //List<String[]> rowResults = readTable(dataPath + "\\" + tableName + ".dat");
        List<String[]> finalResult = new ArrayList<>();
        if (where != null) {
            for (String[] eachRow : tempResult) {
                PrimitiveValue primValue = applyCondition(eachRow, where);
                if(primValue.getType().equals(PrimitiveType.BOOL) && primValue.toBool()){
                    finalResult.add(eachRow);
                } 
            }

        }

        printResult(finalResult, selectItems);
    }

    private static void addColPosWithTabAlias(String tableName, String aliasName, Integer pos) {
        TableSchema selectTableTemp = tables.get(tableName);
        List<String> colNm = selectTableTemp.getListofColumns();
        for (String s : colNm) {
            String colTableMap = aliasName + "." + s;
            colPosWithTableAlias.put(colTableMap, pos);
            pos++;
        }
        System.out.println("Column names with position:" + colPosWithTableAlias);
    }

    private static List<String[]> readTable(String filePath) throws FileNotFoundException {
        File myObj = new File(filePath);
        List<String[]> resultRows = new ArrayList<>();
        try ( Scanner myReader = new Scanner(myObj)) {
            while (myReader.hasNext()) {
                String dataRow = myReader.next();
                resultRows.add(dataRow.split("\\|"));
                //Pair<String, String> key = null;
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
            
        }
        for (String[] result : rowsResult) {

//            for (int i = 0; i < pos.length; i++) {
//                finalREsult[i] = result[pos[i]];
//            }
//            String singleRowResult = "";
//                for (int i=0; i<selectItems.size();i++) {
//                    singleRowResult += "|" + result[i] + "|";
//                }
            System.out.println(String.join("|", result));
        }
        System.out.println("==================================================================================");
    }

    private static String fetchTableNameFromALias(FromItem fromItem) {
        String tableName = fromItem.toString().substring(0, fromItem.toString().indexOf(" AS "));
        return tableName;
    }

    private static PrimitiveValue applyCondition(String[] rowResult, Expression where) throws SQLException {
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {

                System.out.println(column.getTable() + "----" + column.getColumnName());
                int pos = colPosWithTableAlias.get((column.getTable() + "." + column.getColumnName()));
                String value = rowResult[pos];
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
                        System.out.println("this is from int:");
                        System.out.println(new LongValue(value));
                        return new LongValue(value);
                }
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };

        return eval.eval(expressionResolver(where));

    }

    private static Expression expressionResolver(Expression exp) {
        if (exp instanceof Column) {
            System.out.println("Instance of Column :"+expressionResolver(exp));
            return expressionResolver(exp);
        }

        if (exp instanceof DateValue) {
            System.out.println("Instance of Date Value");
            return ((DateValue) exp);
        }
        if (exp instanceof DoubleValue) {
            System.out.println("Instance of Double Value");
            return ((DoubleValue) exp);
        }
        if (exp instanceof NullValue) {
            System.out.println("Instance of Null Value");
            return ((NullValue) exp);
        }
        if (exp instanceof LongValue) {
            System.out.println("Instance of Long Value");
            return ((LongValue) exp);
        }
        if (exp instanceof StringValue) {
            System.out.println("!!!!!!!!sadasdas" + ((StringValue) exp).getValue());
            return ((StringValue) exp);
        }
        if (exp instanceof TimestampValue) {
            System.out.println("Instance of timestamp Value");
            return ((TimestampValue) exp);
        }
        if (exp instanceof TimeValue) {
            System.out.println("Instance of Time Value");
            return ((TimeValue) exp);
        }

        if (exp instanceof GreaterThan) {
            GreaterThan e = (GreaterThan) exp;
            Expression rightExpression = e.getRightExpression();
            Expression leftExpression = e.getLeftExpression();
            System.out.println("Left exp: "+e.getLeftExpression());
            System.out.println("Right exp: "+e.getRightExpression());
            return new GreaterThan(e.getLeftExpression(),e.getRightExpression() );
        }
        if (exp instanceof Addition) {
            Addition e = (Addition) exp;
            System.out.println(e);
        }
        if (exp instanceof OrExpression) {
            OrExpression e = (OrExpression) exp;
            if (e.getRightExpression() instanceof GreaterThan) {
                expressionResolver(e.getRightExpression());
            }
            System.out.println(e);
        }
        return null;
    }

}
