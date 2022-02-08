package org.h2.index;

import org.h2.expression.Expression;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import java.util.ArrayList;

class IndexHelper {
    public static ArrayList<Integer> validateExpressionForCountOperation(Expression[] expressions) {
        int i = 0;
        Aggregate aggregation;
        ArrayList<Integer> countExpressions = new ArrayList<>();
        for (Expression expression : expressions) {
            if (expression instanceof Aggregate) {
                aggregation = (Aggregate) expression;
                if (aggregation.getAggregateType().toString().equals((AggregateType.COUNT).toString())) {
                    countExpressions.add(i);
                }
            }
            i++;
        }
        return countExpressions;
    }



}