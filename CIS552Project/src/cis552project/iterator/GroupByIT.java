package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cis552project.CIS552SO;
import cis552project.ExpressionEvaluator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
//import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

public class GroupByIT extends BaseIT {

	Iterator<TableResult> resIT = null;

	Collection<TableResult> tabResList;

	public GroupByIT(List<Column> groupByColumnList, BaseIT result, CIS552SO cis552so) throws SQLException {
		this.tabResList = new ArrayList<>();

		Map<Tuple, TableResult> groupByMap = new HashMap<>();
		while (result.hasNext()) {
			TableResult initialTabRes = result.getNext();

			PrimitiveValue primValeArray[] = new PrimitiveValue[groupByColumnList.size()];
			for (int i = 0; i < groupByColumnList.size(); i++) {
				Column column = groupByColumnList.get(i);

				Eval eval = new ExpressionEvaluator(initialTabRes.resultTuples, initialTabRes, cis552so);
				primValeArray[i] = eval.eval(column);
			}
			Tuple groupedTuple = new Tuple(primValeArray);
			if (!groupByMap.containsKey(groupedTuple)) {
				TableResult finalTabRes = new TableResult();
				finalTabRes.aliasandTableName.putAll(initialTabRes.aliasandTableName);
				finalTabRes.colPosWithTableAlias.putAll(initialTabRes.colPosWithTableAlias);
				finalTabRes.fromItems.addAll(initialTabRes.fromItems);
				groupByMap.put(groupedTuple, finalTabRes);
			}
			groupByMap.get(groupedTuple).resultTuples.addAll(initialTabRes.resultTuples);
		}
		tabResList = groupByMap.values();
		resIT = tabResList.iterator();

	}

	@Override
	public TableResult getNext() {
		return resIT.next();
	}

	@Override
	public boolean hasNext() {
		return resIT.hasNext();
	}

	@Override
	public void reset() {
		resIT = tabResList.iterator();

	}

}
