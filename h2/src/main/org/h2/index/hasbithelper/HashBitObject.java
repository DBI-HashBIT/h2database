package org.h2.index.hasbithelper;

import org.h2.message.DbException;

import java.io.Serializable;
import java.util.*;

public class HashBitObject implements Serializable {
    public static final int DEFAULT_NUMBER_OF_BUCKETS = 256;

    // set to a power of 2 for uniform distribution
    private int noOfBuckets;

    private final HashMap<Integer, ArrayList<Boolean>> hashBitValues;
    private final List<Long> rowKeys;
    private int length;
    private String tableName;
    private String columnName;

    public HashBitObject() {
        this.hashBitValues = new HashMap<>();
        this.rowKeys = new ArrayList<>();
        length = 0;
    }

    public HashBitObject(int noOfBuckets) {
        this();
        this.noOfBuckets = noOfBuckets;
        System.out.println("HashBitObject created with " + noOfBuckets + " buckets");
    }

    public HashBitObject(int noOfBuckets, String tableName, String columnName) {
        this(noOfBuckets);
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public void add(String value, long rowKey) {
        if (value == null) {
            value = "NULL";
        }
        int hashKey = hash(value);

        if (!hashBitValues.containsKey(hashKey)) {
            hashBitValues.put(hashKey, new ArrayList<>(Collections.nCopies(length, false)));
        }

        // index in the bitmap array
        int index;
        boolean alreadyExists = true;
        if (!rowKeys.contains(rowKey)) {
            alreadyExists = false;
            rowKeys.add(rowKey);
            Collections.sort(rowKeys);
        }
        index = rowKeys.indexOf(rowKey);

        boolean finalAlreadyExists = alreadyExists;
        hashBitValues.forEach((key, keyValue) -> {
            // if a value corresponding to rowKey already exists, we remove it
            // this shifts the elements after the removed element by 1 position to the left
            if (finalAlreadyExists) {
                keyValue.remove(index);
            }
            if (key == hashKey) {
                // this shifts the elements after the added element by 1 position to the right
                keyValue.add(index, true);
            } else {
                // this shifts the elements after the added element by 1 position to the right
                keyValue.add(index, false);
            }
        });

        if (!alreadyExists) length++;
//        System.out.println("Added " + value);
//        System.out.println("row keys : " + rowKeys);
//        System.out.println(this);;
        FileHelper.WriteObjectToFile(getFilePath(), this);
    }


    public void remove(long rowKey) {
        int index = rowKeys.indexOf(rowKey);
        if (index == -1) {
            throw DbException.getInternalError(
                "Value corresponding to the row key [" + rowKey + "] not found in Hashbit index");
        }
        hashBitValues.forEach((key, keyValue) -> {
            keyValue.remove(index);
        });
        rowKeys.remove(index);
        length--;
//        System.out.println("Removed");
//        System.out.println("row keys : " + rowKeys);
//        System.out.println(this);;
        FileHelper.WriteObjectToFile(getFilePath(), this);
    }


    public int getSize() {
        return length;
    }


    public ArrayList<Boolean> getBitmapArray(String key) {
        int hash = hash(key);
        if (hashBitValues.containsKey(hash)) {
            return hashBitValues.get(hash);
        }
        return new ArrayList<>(Collections.nCopies(length, false));
    }


    @Override
    public String toString() {
        StringBuffer string = new StringBuffer();
        hashBitValues.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEachOrdered(mapElement -> {
                    Integer key = mapElement.getKey();
                    string.append(key).append("\t:- [ ");
                    ArrayList<Boolean> keyValue = mapElement.getValue();
                    for (Boolean val: keyValue) {
                        if (val) {
                            string.append("1 , ");
                        } else {
                            string.append("0 , ");
                        }
                    }
                    string.append("]\n");
                });
        return string.toString();
    }

    public void deleteFiles() {
        FileHelper.deleteFiles(getFilePath());
    }

    private String getFilePath() {
        return tableName + "_" + columnName + ".txt";
    }

    private int hash(String key) {
        int hash = (key.hashCode() & 0x7fffffff) % noOfBuckets;
        return hash;
    }

}
