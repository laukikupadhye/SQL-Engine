/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project.iterator;

import cis552project.CIS552SO;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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
            for(OrderByElement orderByElement:orderByElements){
                
            }
            
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }       
       
    }
    
}
