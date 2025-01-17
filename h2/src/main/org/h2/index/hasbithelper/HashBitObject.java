package org.h2.index.hasbithelper;

import java.io.Serializable;
import java.util.*;

public class HashBitObject implements Serializable {
    public static final int DEFAULT_NUMBER_OF_BUCKETS = 256;

    // set to a power of 2 for uniform distribution
    private int noOfBuckets;

    public HashMap<Integer, ArrayList<Boolean>> hashBitValues;
    public int length;
    // Folder name
    private String tableName;
    private String columnName;

    public HashBitObject() {
        this.hashBitValues = new HashMap<>();
        length = 0;
    }

    public HashBitObject(int noOfBuckets) {
        this();
        this.noOfBuckets = noOfBuckets;
        System.out.println("HashBitObject created with " + noOfBuckets + " buckets");
    }

    public HashBitObject(int noOfBuckets, String tableName, String columnName) {
        this();
        this.noOfBuckets = noOfBuckets;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public void add(String value) {
        add(value, -100);
    }

    public void add(String value, long index) {
        if (value == null) {
            value = "NULL";
        }
        int hash = hash(value);

        if (!hashBitValues.containsKey(hash)) {
            hashBitValues.put(hash, new ArrayList<>(Collections.nCopies(length, false)));
        }
        for (Map.Entry<Integer, ArrayList<Boolean>> mapElement : hashBitValues.entrySet()) {
            int key = mapElement.getKey();
            ArrayList<Boolean> keyValue = mapElement.getValue();
            if (index < 0) {
                if (key == hash) {
                    keyValue.add(true);
                } else {
                    keyValue.add(false);
                }
            } else {
                if (key == hash) {
                    keyValue.add(((int) index), true);
                } else {
                    keyValue.add(((int) index), false);
                }
            }
        }
        length++;
        //System.out.println("Added " + value);
        //System.out.println(this);
        FileHelper.WriteObjectToFile(getFilePath()+".txt", this);
    }

    public void update(long index, String newValue, String oldValue) {
        if (oldValue == null) {
            oldValue = "NULL";
        }
        if (newValue == null) {
            newValue = "NULL";
        }

        int oldValueHash = hash(oldValue);
        int newValueHash = hash(newValue);

        if (!hashBitValues.containsKey(newValueHash)) {
            hashBitValues.put(newValueHash, new ArrayList<>(Collections.nCopies(length, false)));
        }
        if (index < 0 || index > length) {
            System.out.println("No key :- " + oldValueHash + " for " + oldValue +  "found in hashbit index");;
        }
        for (Map.Entry<Integer, ArrayList<Boolean>> mapElement : hashBitValues.entrySet()) {
            int key = mapElement.getKey();
            //TODO: Update this code to get the previous one
            ArrayList<Boolean> keyValue = mapElement.getValue();
            if (key == newValueHash) {
                keyValue.set(((int) index), true);
            } else {
                keyValue.set(((int) index), false);
            }
        }
        FileHelper.WriteObjectToFile(getFilePath(), this);
    }

    public void remove(long index) {
        remove(index, false);
    }

    public void remove(long index, Boolean isDelete) {
        // TODO: Index not be the correct one because, primarykey!= table order when we have deleted items in the middle
        if (index < 0 || index > length) {
            System.out.println("No key :- " + index + " found in hashbit index");;
        }
        for (Map.Entry mapElement : hashBitValues.entrySet()) {
            ArrayList<Boolean> keyValue = (ArrayList<Boolean>) mapElement.getValue();
//            TODO: We need to add false if we keep bitmap in primary index order, in that case we need to maintain a deletedIndex array and filter using it for query operation. But it will affect the update methods. So we need to to overwrite methods
            if (isDelete) {
                keyValue.set(((int) index), false);
            } else {
                keyValue.remove(((int) index));
            }
        }
        length--;
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

    private int hash(String key) {
        return (key.hashCode() & 0x7fffffff) % noOfBuckets;
    }

    private Set<Integer> getBucketKeys() {
        return this.hashBitValues.keySet();
    }

    public void deleteFiles() {
        FileHelper.deleteFiles(getFilePath());
    }

    public String getFilePath() {
        return tableName+"_"+columnName;
    }
}
