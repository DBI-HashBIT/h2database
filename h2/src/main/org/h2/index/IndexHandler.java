package org.h2.index;

import org.h2.expression.Expression;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

import java.util.*;

class IndexHelper {
    private String hashBitIndexName  = "hashBitIndex";
    public static ArrayList<Integer> getCountOperationIndexes(Expression[] expressions) {
        int i = 0;
        Column column;
        String columnName;
        Table table;
        Index columnIndex;
        Aggregate aggregateExpression;
        ArrayList<Integer> countExpressions = new ArrayList<>();
        for (Expression expression : expressions) {
            if (expression instanceof Aggregate) {
                aggregateExpression = (Aggregate) expression;
                if (aggregation.getAggregateType().toString().equals((AggregateType.COUNT).toString())) {
                    column = aggregateExpression.getColumnFromAggregateIndex(0);
                    columnName = column.getName();
                    table = column.getTable();
                    columnIndex = table.getIndexForColumn(column, false, false);
                    //TODO Combine indexes
                    if (columnIndex.indexType.equals(hashBitIndexName) && columnIndex.indexColumns.length == 1) {
                        countExpressions.add(i);
                    }
                }
            }
            i++;
        }
        return countExpressions;
    }

    public HashMap<Integer, ArrayList<Integer>> getValueForCountOperationWithHashBitIndexes(int[] countIndexes, Expression[] expressions) {
        HashMap<Integer, ArrayList<Integer>> results = new HashMap<>();
        int[] tempBitmapArray= new int[]{0, 1, 0};
        Column column;
        String columnName;
        Table table;
        Index columnIndex;
        Aggregate aggregateExpression;
        for (int index : countIndexes) {
            aggregateExpression = (Aggregate) (expressions.get(index));
            column = aggregateExpression.getColumnFromAggregateIndex(0);
            columnName = column.getName();
            table = column.getTable();
            columnIndex = table.getIndexForColumn(column, false, false);
            TransactionMap<SearchRow, Value> dataMap = columnIndex.getMap();
//            get the bitmap indices array;
//            dataMap.get(column);
            results.put(index, new ArrayList<>(Array.asList(tempBitmapArray)));
        }
        return results;
    }

    public String getHashBitIndexName() {
        return hashBitIndexName;
    }

    public void setHashBitIndexName(String hashBitIndexName) {
        this.hashBitIndexName = hashBitIndexName;
    }
}