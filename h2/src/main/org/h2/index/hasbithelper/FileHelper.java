package org.h2.index.hasbithelper;

import java.io.*;

public class FileHelper {
    private static String directoryName = "D:\\Acedemic\\UOM\\Semesters\\Semester 8\\Database Internals\\Project\\Forked-H2Database\\h2database\\h2\\src\\main\\org\\h2\\index\\hasbithelper";
    private static String separator = "/";

    public static void WriteObjectToFile(String filepath, HashBitObject serObj) {
        try {
            FileOutputStream fileOut = new FileOutputStream(filepath);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(serObj);
            objectOut.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static HashBitObject ReadObjectFromFile(String filepath) {
        try {
            FileInputStream fileIn = new FileInputStream(filepath);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Object obj = objectIn.readObject();
            objectIn.close();
            return (HashBitObject) obj;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static void addNewHashObject(String tableName, String columnName) {
        String directoryName = FileHelper.directoryName;
        String fileName = tableName + "_" + columnName + ".txt";

        File directory = new File(directoryName);
        if (! directory.exists()){
            directory.mkdir();
        }
        File file = new File(directoryName + separator + fileName);
        WriteObjectToFile(file.getAbsolutePath(), new HashBitObject());
    }
}
