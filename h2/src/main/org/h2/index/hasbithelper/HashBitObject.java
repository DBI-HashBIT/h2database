package org.h2.index.hasbithelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class HashBitObject implements Serializable {
    public HashMap<String, ArrayList<Boolean>> hashBitValues;

    public HashBitObject() {
        this.hashBitValues = new HashMap<>();
    }

    public void add(String value) {
        for (Map.Entry mapElement : hashBitValues.entrySet()) {
            String key = (String)mapElement.getKey();
            ArrayList<Boolean> keyValue = (ArrayList<Boolean>) mapElement.getValue();
            if (key.equals(value)) {
                keyValue.add(true);
            } else {
                keyValue.add(false);
            }
        }
    }

    public void update(int index, String value) {
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
        for (Map.Entry mapElement : hashBitValues.entrySet()) {
            String key = (String)mapElement.getKey();
            ArrayList<Boolean> keyValue = (ArrayList<Boolean>) mapElement.getValue();
            keyValue.remove(index);
        }
    }
}
