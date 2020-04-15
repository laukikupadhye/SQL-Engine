/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project.iterator;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 *
 * @author melvi
 */
public class OrderByIT extends BaseIT {
    
        TableResult finalTableResult = null;
        Iterator<Tuple> resIT = null;
        List<Tuple> finalResultTuples = null;
        List<OrderByElement> orderByElements;
        CIS552SO cis552SO;
        
    
        public OrderByIT(List<OrderByElement> orderByElements, BaseIT result, CIS552SO cis552SO) {
            this.cis552SO = cis552SO;
            this.orderByElements = orderByElements;
            while (result.hasNext()){
                    TableResult initialTabRes = result.getNext();
                    if(finalTableResult == null){
                        finalTableResult = new TableResult();
                        finalTableResult.aliasandTableName.putAll(initialTabRes.aliasandTableName);
                        finalTableResult.colPosWithTableAlias.putAll(initialTabRes.colPosWithTableAlias);
                        finalTableResult.fromItems.addAll(initialTabRes.fromItems);
                         
                    }
                    finalResultTuples.addAll(initialTabRes.resultTuples);
            }  
            
        finalResultTuples.sort(new TupleCompare());
            resIT = finalResultTuples.iterator();
            
            
            
       }
        
    @Override
    public TableResult getNext() {
            finalTableResult.resultTuples = new ArrayList<>();
            finalTableResult.resultTuples.add(resIT.next());
            return finalTableResult;
    }

    @Override
    public boolean hasNext() {
            return resIT.hasNext();
    }

    @Override
    public void reset() {
            resIT = finalResultTuples.iterator();
    }

    private class TupleCompare implements Comparator<Tuple> {
        @Override
        public int compare(Tuple t1, Tuple t2) {
            int compare = 0;
            for(OrderByElement orderByElement:orderByElements){
                try {
                    Eval eval1 = new ExpressionEvaluator(Arrays.asList(t1), finalTableResult, cis552SO);
                    Eval eval2 = new ExpressionEvaluator(Arrays.asList(t2), finalTableResult, cis552SO);
                    PrimitiveValue pval1 = eval1.eval(orderByElement.getExpression());
                    PrimitiveValue pval2 =  eval2.eval(orderByElement.getExpression());
                    
                    compare = pval1.toRawString().compareToIgnoreCase(pval2.toRawString());
                    if(compare != 0){
                        return compare;
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(OrderByIT.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return compare;
        }       
       
    }
    
}
