package iterator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import cis552project.CIS552SO;
import cis552project.TableColumnData;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectItemIT extends BaseIT {

	BaseIT result = null;
	List<SelectItem> selectItems = null;
	CIS552SO cis552so = null;

	public SelectItemIT(List<SelectItem> selectItems, BaseIT result, CIS552SO cis552so) {
		this.selectItems = selectItems;
		this.result = result;
		this.cis552so = cis552so;
	}

	@Override
	public TableResult getNext() {
		try {
			return solveSelectItemExpression();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		if (result == null || !result.hasNext()) {
			return false;
		}
		return true;
	}

	@Override
	public void reset() {
	}

	private TableResult solveSelectItemExpression() throws SQLException {

		if (selectItems.get(0) instanceof AllColumns) {
			return result.getNext();
		}

		TableResult tabRes = new TableResult();
		List<Expression> finalExpItemList = new ArrayList<>();
		for (SelectItem selectItem : selectItems) {
			if (selectItem instanceof SelectExpressionItem) {
				if (((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
					Function funExp = (Function) ((SelectExpressionItem) selectItem).getExpression();
					String funName = funExp.getName().toUpperCase();
					switch (funName) {
					case "COUNT":
						if (funExp.isAllColumns()) {
							tabRes.getResultTuples()
									.add(new String[] { Integer.toString(tabRes.getResultTuples().size()) });
							return tabRes;
						}
					}

				}
				finalExpItemList.add(((SelectExpressionItem) selectItem).getExpression());
			} else if (selectItem instanceof AllTableColumns) {
				AllTableColumns allTableColumn = (AllTableColumns) selectItem;
				TableColumnData tableSchema = cis552so.tables
						.get(tabRes.getAliasandTableName().get(allTableColumn.getTable().getName()));

				finalExpItemList.addAll(tableSchema.getListofColumns().stream()
						.map(col -> (Expression) new Column(allTableColumn.getTable(), col.getColumnName()))
						.collect(Collectors.toList()));
			}
		}
		for (String[] resultRow : result.getNext().getResultTuples()) {
			String[] primValToString = new String[finalExpItemList.size()];
			for (int i = 0; i < finalExpItemList.size(); i++) {

				PrimitiveValue value = ExpressionEvaluator.applyCondition(resultRow, finalExpItemList.get(i), tabRes,
						cis552so);
				primValToString[i] = value.toRawString();

			}
			tabRes.getResultTuples().add(primValToString);
		}
		return tabRes;
	}

}
