package cis552project.iterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cis552project.CIS552SO;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
//import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByIT extends BaseIT {

    TableResult finalTableResult = null;
    Iterator<Tuple> resIT = null;
    List<Tuple> finalResultTuples = null;
    
    
    
    Map<PrimitiveValue[], List<Tuple>> groupByMap = new HashMap<>();

    public GroupByIT(List<Column> groupByColumnList, List<SelectItem> selectItems, BaseIT result, CIS552SO cis552so) throws SQLException {
        copyAllResults(result);
        List<Tuple> resultCombiningSelect=new ArrayList<>();
        for (Tuple tuple : finalResultTuples) {
            PrimitiveValue primValeArray[] = new PrimitiveValue[groupByColumnList.size()];
            int i = 0;
            for (Column column : groupByColumnList) {
                primValeArray[i] = ExpressionEvaluator.applyCondition(tuple.resultRow, (Expression) column, finalTableResult, cis552so);
                i++;
            }
            if (!groupByMap.containsKey(primValeArray)) {
                System.out.println("key:"+primValeArray[0].toRawString());
                groupByMap.put(primValeArray, new ArrayList<>());
            }
            groupByMap.get(primValeArray).add(tuple);
            
        }
        for (Map.Entry<PrimitiveValue[], List<Tuple>> keyValuePair : groupByMap.entrySet()) {
            PrimitiveValue[] functionSolution = evaluateFunction(keyValuePair.getKey(),keyValuePair.getValue(), selectItems,groupByColumnList ,cis552so);
            resultCombiningSelect.add(new Tuple(functionSolution));
        }
        finalResultTuples=resultCombiningSelect;
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

    private List<Tuple> applyFuntion(List<Tuple> resultTuples) {

        return null;//throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void copyAllResults(BaseIT result) {
        List<Tuple> resultTuples = new ArrayList<>();
        while (result.hasNext()) {
            TableResult initialTabRes = result.getNext();
            if (finalTableResult == null) {
                finalTableResult = new TableResult();
                finalTableResult.aliasandTableName.putAll(initialTabRes.aliasandTableName);
                //finalTableResult.colDefMap.putAll(initialTabRes.colDefMap);
                finalTableResult.colPosWithTableAlias.putAll(initialTabRes.colPosWithTableAlias);
                finalTableResult.fromItems.addAll(initialTabRes.fromItems);
            }
            resultTuples.addAll(initialTabRes.resultTuples);
        }
        finalResultTuples = resultTuples;
    }

    private PrimitiveValue[] evaluateFunction(PrimitiveValue[] groupByKey, List<Tuple> rowByEachKey, List<SelectItem> selectItems, List<Column> groupByColumnList, CIS552SO cis552so) throws SQLException {
        PrimitiveValue[] valueAfterFuntion = new PrimitiveValue[selectItems.size()];
        int i = 0;
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExp = (SelectExpressionItem) selectItem;
                Expression exp = selectExp.getExpression();
                if (exp instanceof Function) {
                    valueAfterFuntion[i] = FunctionEvaluation.applyFunction(rowByEachKey, (Function) exp, finalTableResult, cis552so);
                    i++;
                }else if(exp instanceof Column){
                    int j=0;
                    for (Column column : groupByColumnList) {
                        if(column.getColumnName().equals(((Column) exp).getColumnName())){
                            valueAfterFuntion[i]=groupByKey[j];
                            i++;
                            j++;
                        }
                        else {
                            j++;
                        }
                    }
                    //checkIfFunctionExist();
                    //valueAfterFuntion[i]=ExpressionEvaluator.applyCondition(valueAfterFuntion, exp, finalTableResult, cis552so)
                }
            }
        }
        return valueAfterFuntion;
    }

    private PrimitiveValue[] createFinalTupel(PrimitiveValue[] key, PrimitiveValue[] functionSolution) {
        PrimitiveValue[] afterCombine=null;
        for (int i = 0; i < key.length; i++) {
            afterCombine[i]=key[i];
        }
        for (int i = afterCombine.length, j=0; i < afterCombine.length+functionSolution.length; i++,j++) {
            afterCombine[i]=functionSolution[j];
        }
        return afterCombine;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
