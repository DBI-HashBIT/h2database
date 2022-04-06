package org.h2.index;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ValueExpression;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.mvstore.db.HashBitIndex;
import org.h2.table.Column;
import org.h2.table.Table;

import java.util.*;

public class IndexHandler {

    private static ArrayList<Boolean> getBitMapIndices(HashBitIndex hashBitIndex, String value) {
        ArrayList<Boolean> bitmapArray = hashBitIndex.getBitMapArray(value);
        return bitmapArray;
    }

    //TODO: When there is another funcs with count, there can be errors so handle these (Expression == 1) things or Do count separtly and remove that expression
    public static ArrayList<Boolean> comparisonOperationIndexes(Expression expression, SessionLocal session) {
        int i = 0;
        Column column;
        String columnName;
        Table table;
        Index columnIndex;
        Comparison comparison;
        ArrayList<Integer> andOrExpressions = new ArrayList<>();
        if (expression instanceof Comparison) {
            comparison = (Comparison) expression;
            //TODO Implement this for nested values
            if (comparison.getLeft() instanceof ExpressionColumn) {
                column = ((ExpressionColumn)(comparison.getLeft())).getColumn();
                columnName = column.getName();
                table = column.getTable();
                columnIndex = table.getIndexForColumn(column, false, false);
                if (columnIndex != null
                    && comparison.getRight() instanceof ValueExpression
                    && comparison.getCompareType() ==  Comparison.EQUAL
                    && columnIndex .indexType.isHashbit()
                    && columnIndex.indexColumns.length == 1) {
                        return getBitMapIndices((HashBitIndex) columnIndex, comparison.getRight().getValue(session).getString());
                }
            }
        }
        return null;
    }

    //TODO: When there is another funcs with AND/OR, there can be errors so handle these (Expression == 1) things or Do count separtly and remove that expression
    public static ArrayList<Boolean> andOrOperationIndexes(Expression expression, SessionLocal session) {
        ArrayList<Boolean> resultBitmap = new ArrayList<>(), left = null, right = null, results = null, values = null,
                tempresults = null;
        ConditionAndOr conditionAndOr;
        ConditionAndOrN conditionAndOrn;
        Expression ex;
        if (expression instanceof Comparison) {
            results = comparisonOperationIndexes(expression, session);
        } else if (expression instanceof ConditionAndOr) {
            conditionAndOr = (ConditionAndOr) expression;
            left = andOrOperationIndexes(conditionAndOr.getLeft(), session);
            right = andOrOperationIndexes(conditionAndOr.getRight(), session);
            if (left != null && right != null) {
                results = andOrBitMap(left, right, conditionAndOr.getAndOrType());
            }
        } else if (expression instanceof ConditionAndOrN) {
            conditionAndOrn = (ConditionAndOrN) expression;
            for (int i = 0; i < conditionAndOrn.getSubexpressionCount(); i++) {
                ex = conditionAndOrn.getSubexpression(i);
                tempresults = andOrOperationIndexes(ex, session);
                if (tempresults != null) {
                    if (results == null) {
                        results = tempresults;
                    } else {
                        results = andOrBitMap(results, tempresults, conditionAndOrn.getAndOrType());
                    }
                } else {
                    return null;
                }
            }
        }
        return results;
    }

    public static ArrayList<Boolean> andOrBitMap(ArrayList<Boolean> left, ArrayList<Boolean> right, int type) {
        ArrayList<Boolean> results = new ArrayList<>();
        for(int i = 0; i < left.size(); i++) {
            if (type == ConditionAndOr.OR) {
                results.add(left.get(i) || right.get(i));
            } else if (type == ConditionAndOr.AND) {
                results.add(left.get(i) && right.get(i));
            }
        }
        return results;
    }
}