package org.h2.index.hasbithelper;

import java.io.Serializable;
import java.util.*;

public class HashBitObject implements Serializable {
    private static final int NO_OF_BUCKETS = 256;

    public HashMap<Integer, ArrayList<Boolean>> hashBitValues;
    public int length;

    public HashBitObject() {
        this.hashBitValues = new HashMap<>();
        length = 0;
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
                    keyValue.add(((int) index) - 1, true);
                } else {
                    keyValue.add(((int) index) - 1, false);
                }
            }
        }
        length++;
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
                keyValue.set(((int) index) - 1, true);
            } else {
                keyValue.set(((int) index) - 1, false);
            }
        }
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
                keyValue.set(((int) index) - 1, false);
            } else {
                keyValue.remove(((int) index) - 1);
            }
        }
        length--;
    }

    public int getSize() {
        return length;
    }

    public ArrayList<Boolean> getBitmapArray(String key) {
        if (hashBitValues.containsKey(key)) {
            return hashBitValues.get(key);
        }
        return null;
    }

    @Override
    public String toString() {
        String string = "";
        for (Map.Entry mapElement : hashBitValues.entrySet()) {
            String key = (String)mapElement.getKey();
            string += key;
            string += " :- [";
            ArrayList<Boolean> keyValue = (ArrayList<Boolean>) mapElement.getValue();
            for (Boolean val: keyValue) {
                if (val) {
                    string += "1 , ";
                } else {
                    string += "0 , ";
                }
            }
            string = string.substring(0, string.length() - 2);
            string += "]\n";
        }
        return string;
    }

    public int hash(String key) {
        return key.hashCode() % NO_OF_BUCKETS;
    }


}
