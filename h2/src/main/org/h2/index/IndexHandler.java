package org.h2.index;

import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ValueExpression;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

import java.util.*;

public class IndexHandler {
    private static String hashBitIndexName  = "hashBitIndex";
    private static int[] outerAndOrTempBitmapArray = new int[]{1, 1, 1, 1, 0, 0, 0, 1};

    private static ArrayList<Integer> getBitMapIndices(Column column, Table table) {
        ArrayList<Integer> integerArray = new ArrayList<>(outerAndOrTempBitmapArray.length);
        for (int j : outerAndOrTempBitmapArray) {
            integerArray.add(j);
        }
        return integerArray;
    }

    //TODO: When there is another funcs with count, there can be errors so handle these (Expression == 1) things or Do count separtly and remove that expression
    public static ArrayList<Integer> comparisonOperationIndexes(Expression expression) {
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
                if (comparison.getRight() instanceof ValueExpression &&
                        (comparison.getCompareType() ==  Comparison.EQUAL) &&
                        (true || columnIndex.indexType.equals(hashBitIndexName) &&
                                columnIndex.indexColumns.length == 1)) {
                    //Get Bitmap Indices
                    return getBitMapIndices(column, table);
                }
            }
        }
        return null;
    }

    //TODO: When there is another funcs with AND/OR, there can be errors so handle these (Expression == 1) things or Do count separtly and remove that expression
    public static ArrayList<Integer> andOrOperationIndexes(Expression expression) {
        ArrayList<Integer> resultBitmap = new ArrayList<>(), left = null, right = null, results = null, values = null,
                tempresults = null;
        ConditionAndOr conditionAndOr;
        ConditionAndOrN conditionAndOrn;
        Expression ex;
        if (expression instanceof Comparison) {
            results = comparisonOperationIndexes(expression);
        } else if (expression instanceof ConditionAndOr) {
            conditionAndOr = (ConditionAndOr) expression;
            left = andOrOperationIndexes(conditionAndOr.getLeft());
            right = andOrOperationIndexes(conditionAndOr.getRight());
            if (left != null && right != null) {
                results = andOrBitMap(left, right, conditionAndOr.getAndOrType());
            }
        } else if (expression instanceof ConditionAndOrN) {
            conditionAndOrn = (ConditionAndOrN) expression;
            for (int i = 0; i < conditionAndOrn.getSubexpressionCount(); i++) {
                ex = conditionAndOrn.getSubexpression(i);
                tempresults = andOrOperationIndexes(ex);
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

    public static ArrayList<Integer> andOrBitMap(ArrayList<Integer> left, ArrayList<Integer> right, int type) {
        ArrayList<Integer> results = new ArrayList<>();
        for(int i = 0; i < left.size(); i++) {
            if (type == ConditionAndOr.OR) {
                results.add(Math.min(1, left.get(i) + right.get(i)));
            } else if (type == ConditionAndOr.AND) {
                results.add(left.get(i) * right.get(i));
            }
        }
        return results;
    }

    public String getHashBitIndexName() {
        return hashBitIndexName;
    }

    public void setHashBitIndexName(String hashBitIndexName) {
        this.hashBitIndexName = hashBitIndexName;
    }

 
    private int attributeExists(String attr) {
        //change to Value - String is just to test
        String[] bitmap_attrs=new String[]{"first","second","third","fourth","fifth"};
        //iterate through existing rows
        for (int i = 0; i < bitmap_attrs.length; i++) {
            String v1 = bitmap_attrs[i];
            String v2 = attr;
            if (Objects.equals(v1, v2)) {
                return i; //rerurn place or row of existing attribute
            }
        }
        return -1;
    }

    public ArrayList<ArrayList<Integer>> insertRowBitMap(String attr) {
        //get the feature of inserting row
        //get the rows check for the feature, 
        ArrayList<ArrayList<Integer>> bitmap_all = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> first = new ArrayList<>(Arrays.asList(1, 0, 0, 0, 0, 1, 0, 0));
        ArrayList<Integer> second = new ArrayList<>(Arrays.asList(0, 1, 0, 0, 0, 0, 0, 1));
        ArrayList<Integer> third = new ArrayList<>(Arrays.asList(0, 0, 1, 0, 0, 0, 1, 0));
        ArrayList<Integer> fourth = new ArrayList<>(Arrays.asList(0, 0, 0, 1, 0, 0, 0, 0));
        ArrayList<Integer> fifth = new ArrayList<>(Arrays.asList(0, 0, 0, 0, 1, 0, 0, 0));
        bitmap_all.add(first);
        bitmap_all.add(second);
        bitmap_all.add(third);
        bitmap_all.add(fourth);
        bitmap_all.add(fifth);
        System.out.print("bitmap_all");
        System.out.println(bitmap_all);
        int place = attributeExists(attr);
        System.out.print("does exist");
        System.out.println(place);
        for(int i = 0; i < bitmap_all.size(); i++) {
            ArrayList<Integer> attr_list = bitmap_all.get(i);
            if (i==place){
                attr_list.add(1);
            }
            else{
                attr_list.add(0);
            }

        }
        if (place==-1){
            System.out.println("new attribute");
            //add new attribute 
        }
        return bitmap_all;
    }
    //search how to get all entries
    

    public ArrayList<ArrayList<Integer>> deleteRowBitMap(String attr, int rowPlace) {
        //get the feature of inserting row
        //get the rows check for the feature, 
        ArrayList<ArrayList<Integer>> bitmap_all = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> bitmap_all_new = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> first = new ArrayList<>(Arrays.asList(1, 0, 0, 0, 0, 1, 0, 0));
        ArrayList<Integer> second = new ArrayList<>(Arrays.asList(0, 1, 0, 0, 0, 0, 0, 1));
        ArrayList<Integer> third = new ArrayList<>(Arrays.asList(0, 0, 1, 0, 0, 0, 1, 0));
        ArrayList<Integer> fourth = new ArrayList<>(Arrays.asList(0, 0, 0, 1, 0, 0, 0, 0));
        ArrayList<Integer> fifth = new ArrayList<>(Arrays.asList(0, 0, 0, 0, 1, 0, 0, 0));
        bitmap_all.add(first);
        bitmap_all.add(second);
        bitmap_all.add(third);
        bitmap_all.add(fourth);
        bitmap_all.add(fifth);
        System.out.print("bitmap_all");
        System.out.println(bitmap_all);

        //get attribute place
        int place = attributeExists(attr);
        System.out.print("attr place");
        System.out.println(place);
        int numItems = 2;
        for(int i = 0; i < bitmap_all.size(); i++) {
            ArrayList<Integer> attr_bitmap_list = bitmap_all.get(i);
            if (i == place){
                //get the count of rows with attribute
                //expected numItems to be one if the attribute exists only in the deleted row
                numItems = Collections.frequency(attr_bitmap_list, 1);
            }
            attr_bitmap_list.remove(rowPlace);
            //if numItems is greater than one add the attribute to the new list
            //could be faster if we just remove attribute if numItems is 1
            if (numItems > 1){
                bitmap_all_new.add(attr_bitmap_list);
            } 
            
        }

        if (place==-1){
            System.out.println("Error; attribute cannot be found");
        }

        
        return bitmap_all_new;
    }

    public ArrayList<ArrayList<Integer>> updateRowBitMap(String new_attr, int rowPlace, String old_attr) {
        
        ArrayList<ArrayList<Integer>> bitmap_all_new = new ArrayList<ArrayList<Integer>>();

        
        bitmap_all_new = deleteRowBitMap(old_attr, rowPlace); //pass the array -- only a dummy array is used in delete function
        insertRowBitMap(new_attr); //pass the array -- only a dummy array is used in insert function

        return bitmap_all_new;
    }
    

}