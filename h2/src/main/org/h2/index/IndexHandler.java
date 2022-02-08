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

public class IndexHandler {
    private static String hashBitIndexName  = "hashBitIndex";
    public static ArrayList<Integer> getCountOperationIndexes(ArrayList<Expression> expressions) {
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
                if (aggregateExpression.getAggregateType().toString().equals((AggregateType.COUNT).toString())) {
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

    public static HashMap<Integer, ArrayList<Integer>> getValueForCountOperationWithHashBitIndexes(
            ArrayList<Integer> countIndexes, ArrayList<Expression> expressions) {
        HashMap<Integer, ArrayList<Integer>> results = new HashMap<>();
        int[] tempBitmapArray= new int[]{0, 1, 0};
        Column column;
        String columnName;
        Table table;
        Index columnIndex;
        Aggregate aggregateExpression;
        ArrayList<Integer> integerArray;
        for (int index : countIndexes) {
            aggregateExpression = (Aggregate) (expressions.get(index));
            column = aggregateExpression.getColumnFromAggregateIndex(0);
            columnName = column.getName();
            table = column.getTable();
            columnIndex = table.getIndexForColumn(column, false, false);
            TransactionMap<SearchRow, Value> dataMap = columnIndex.getTransactionMapMap(null);
//            get the bitmap indices array;
//            dataMap.get(column);
            integerArray = new ArrayList<Integer>(tempBitmapArray.length);
            for (int i : tempBitmapArray) {
                integerArray.add(i);
            }
            results.put(index, (integerArray));
        }
        return results;
    }

    //TODO: Check this
    public static ArrayList<Integer> combineBitmaps(HashMap<Integer, ArrayList<Integer>> bitmaps) {
        ArrayList<Integer> combinebitmap = null;
        ArrayList<Integer> tempmap = null;
        ArrayList<Integer> values;

        while (bitmaps.hasNext()) {
            Map.Entry mapElement
                    = (Map.Entry)bitmaps.next();
            values = (ArrayList<Integer>) mapElement.getValue();
            if (combinebitmap == null) {
                combinebitmap = (ArrayList<Integer>) values.clone();
                tempmap = (ArrayList<Integer>) values.clone();
            } else {
                combinebitmap = new ArrayList<>();
                for (int i = 0; i< values.size(); i++) {
                    if(values.get(i) == 1 || tempmap.get(i) == 1) {
                        combinebitmap.add(1);
                    } else {
                        combinebitmap.add(0);
                    }
                }
                tempmap = (ArrayList<Integer>) combinebitmap.clone();
            }
        }
        return combinebitmap;
    }

    public String getHashBitIndexName() {
        return hashBitIndexName;
    }

    public void setHashBitIndexName(String hashBitIndexName) {
        this.hashBitIndexName = hashBitIndexName;
    }
}