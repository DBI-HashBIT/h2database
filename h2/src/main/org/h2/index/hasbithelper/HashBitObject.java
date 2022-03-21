package org.h2.index.hasbithelper;

import java.io.Serializable;
import java.util.*;

public class HashBitObject implements Serializable {
    public HashMap<String, ArrayList<Boolean>> hashBitValues;
    public int length;

    public HashBitObject() {
        this.hashBitValues = new HashMap<>();
        length = 0;
    }

    public void add(String value) {
        if (!hashBitValues.containsKey(value)) {
            hashBitValues.put(value, new ArrayList<>(Collections.nCopies(length, false)));
        }
        for (Map.Entry mapElement : hashBitValues.entrySet()) {
            String key = (String)mapElement.getKey();
            ArrayList<Boolean> keyValue = (ArrayList<Boolean>) mapElement.getValue();
            if (key.equals(value)) {
                keyValue.add(true);
            } else {
                keyValue.add(false);
            }
        }
        length++;
    }

    public void update(int index, String value) {
        if (!hashBitValues.containsKey(value)) {
            System.out.println("No key :- " + value + " found in hashbit index");;
        }
        for (Map.Entry mapElement : hashBitValues.entrySet()) {
            String key = (String)mapElement.getKey();
            ArrayList<Boolean> keyValue = (ArrayList<Boolean>) mapElement.getValue();
            if (key.equals(value)) {
                keyValue.set(index, true);
            } else {
                keyValue.set(index, false);
            }
        }
    }

    public void remove(int index) {
        if (!hashBitValues.containsKey(index)) {
            System.out.println("No key :- " + index + " found in hashbit index");;
        }
        for (Map.Entry mapElement : hashBitValues.entrySet()) {
            String key = (String)mapElement.getKey();
            ArrayList<Boolean> keyValue = (ArrayList<Boolean>) mapElement.getValue();
            keyValue.remove(index);
        }
        length--;
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
}
