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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 *
 * @author RAD
 */
public class CIS552Project {

    static String dataPath = null;
    static String commandsLoc = null;
    static Map<String, List<ColumnDefinition>> tables = new HashMap<>();
    
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
                System.out.println(statement);
                if (statement instanceof Select) {
                    Select select = (Select) statement;
                    SelectBody selectBody = select.getSelectBody();
                    if (selectBody instanceof PlainSelect) {
                        PlainSelect plainSelect = (PlainSelect) selectBody;
                        List<SelectItem> selectItems = plainSelect.getSelectItems();
                        List<Join> joins = plainSelect.getJoins();
                        FromItem fromItem = plainSelect.getFromItem();
                        System.out.println(fromItem);
                        System.out.println(selectItems);
                        System.out.println(joins);
                        //readTable(dataPath);
                    }
                } else if (statement instanceof CreateTable) {
                    CreateTable createTable = (CreateTable) statement;
                    List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
                    tables.put(createTable.getTable().getWholeTableName(), columnDefinitions);
                }
            }
            for (Map.Entry<String, List<ColumnDefinition>> entry : tables.entrySet()) {
                String key = entry.getKey();
                List<ColumnDefinition> value = entry.getValue();
                System.out.println(key + value);
                
            }
        } catch (ParseException | FileNotFoundException ex) {
            Logger.getLogger(CIS552Project.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static List<String> readCommands(String filePath) throws FileNotFoundException {
        File myObj = new File(filePath);
        List<String> statements = new ArrayList<>();
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
                statements.add(statement);
            }
        }
        return statements;
    }
    
    private static void readTable(String filePath) throws FileNotFoundException {
        File myObj = new File(filePath);
        List<String> statements = new ArrayList<>();
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
                statements.add(statement);
            }
        }
    }
    
}
