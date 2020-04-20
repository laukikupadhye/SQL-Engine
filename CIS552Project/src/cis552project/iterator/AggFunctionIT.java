package cis552project.iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AggFunctionIT extends BaseIT {

	List<TableResult> finalTableResultList;
	Iterator<TableResult> resIT;

	public AggFunctionIT(BaseIT result) throws SQLException {
		List<Tuple> resultTuples = new ArrayList<>();

		finalTableResultList = new ArrayList<>();
		TableResult initialTabRes = null;
		while (result.hasNext()) {
			initialTabRes = result.getNext();
			resultTuples.addAll(initialTabRes.resultTuples);
		}
		initialTabRes.resultTuples = resultTuples;
		finalTableResultList.add(initialTabRes);
		resIT = finalTableResultList.iterator();
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
		resIT = finalTableResultList.iterator();
	}

}
