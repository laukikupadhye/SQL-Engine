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
import java.util.List;
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
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.WithItem;

/**
 *
 * @author RAD
 */
public class CIS552Project {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        //Reader reader = new StringReader("Select R.* FROM R , S");
        Reader reader = new StringReader("CREATE TABLE R(A int, B int)");
        CCJSqlParser parser = new CCJSqlParser(reader);
        try {
            Statement statement = parser.Statement();
            System.out.println(statement);
            if(statement instanceof Select){
                Select select = (Select) statement;
                SelectBody selectBody   = select.getSelectBody();
                if(selectBody instanceof PlainSelect){
                    PlainSelect plainSelect = (PlainSelect) selectBody;
                    List<SelectItem> selectItems = plainSelect.getSelectItems();
                    List<Join> joins = plainSelect.getJoins();
                    FromItem fromItem = plainSelect.getFromItem();
                    System.out.println(fromItem);
                    System.out.println(selectItems);
                    System.out.println(joins);
                    fileRead();
                }
            }else if(statement instanceof CreateTable){
                CreateTable createTable= (CreateTable) statement;
                List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
                System.out.println(columnDefinitions);
            }
        } catch (ParseException ex) {
            Logger.getLogger(CIS552Project.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CIS552Project.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private static void fileRead() throws FileNotFoundException{
        File myObj = new File("data\\R.dat");
        Scanner myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            String[] split = data.split("\\|");
            for (String item:split){
                System.out.println(item);
            }
            System.out.println(data + " ---> " +split.length + " --- " + split[0] + "-" + split[1]);
      }
      myReader.close();

    }
}
