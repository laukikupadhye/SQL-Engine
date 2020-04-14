/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cis552project.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.statement.select.Limit;

/**
 *
 * @author anush
 */
public class LimitIT extends BaseIT{
   Limit limit;
   long current_position = 0;
   List<Tuple> resultTuples = new ArrayList<>();
   BaseIT result = null;
   
        LimitIT(Limit limit, BaseIT result) {
            this.limit = limit;
            this.result = result;
        }  

    @Override
    public TableResult getNext() {
    current_position++;
    return result.getNext();
    }

    @Override
    public boolean hasNext() {
    if(current_position == limit.getRowCount() || result == null || !result.hasNext()){
        return false;
    }
    return true;
    }

    @Override
    public void reset() {
        current_position = 0;
        result.reset();
    }
      
}