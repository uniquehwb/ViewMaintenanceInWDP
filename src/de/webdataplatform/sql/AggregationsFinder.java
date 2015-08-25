package de.webdataplatform.sql;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

public class AggregationsFinder implements SelectVisitor {

	private List<Function> aggregations;

	public List<Function> getAggregationList(PlainSelect plainSelect) {
		aggregations = new ArrayList<Function>();
		plainSelect.accept(this);
		return aggregations;

	}

	@Override
	public void visit(PlainSelect plainSelect) {
		// TODO Auto-generated method stub
		List<SelectItem> selectItems = plainSelect.getSelectItems();
		processSelectClause(selectItems);
	}

	@Override
	public void visit(SetOperationList arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WithItem arg0) {
		// TODO Auto-generated method stub

	}

	private void processSelectClause(List<SelectItem> selectItems) {
		for (SelectItem si : selectItems) {
			if (si instanceof SelectExpressionItem) {
				Expression selectExpr = ((SelectExpressionItem) si).getExpression();
				if (selectExpr instanceof Function) {
					Function fun = (Function) selectExpr;
					if (fun.getName().equalsIgnoreCase("SUM")) {
						aggregations.add(fun);
					} else if (fun.getName().equalsIgnoreCase("COUNT")) {
						aggregations.add(fun);
					} 
				}
			}
		}
	}

}
