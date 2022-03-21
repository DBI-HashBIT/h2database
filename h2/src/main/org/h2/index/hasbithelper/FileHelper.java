package org.h2.index.hasbithelper;

import org.h2.table.Column;

import java.io.*;

public class FileHelper {
    private static String directoryName = "D:\\Acedemic\\UOM\\Semesters\\Semester 8\\Database Internals\\Project\\Forked-H2Database\\h2database\\h2\\src\\main\\org\\h2\\index\\hasbithelper";
    private static String separator = "/";

    public static void WriteObjectToFile(String filepath, HashBitObject serObj) {
        try {
            filepath = directoryName + separator + filepath;
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
            filepath = directoryName + separator + filepath;
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

    private static String generateColumnNames(Column[] columns) {
        String columnName = "";
        for (Column column: columns) {
            columnName += column.getName()+ ",";
        }
        columnName = columnName.substring(0, columnName.length() - 1);
        return columnName;
    }

    public static void addNewHashObject(String tableName, Column[] columns) {
        String directoryName = FileHelper.directoryName;
        String fileName = generateFileName(tableName, columns);

        File directory = new File(directoryName);
        if (! directory.exists()){
            directory.mkdir();
        }
        File file = new File(directoryName + separator + fileName);
        WriteObjectToFile(fileName, new HashBitObject());
    }

    public static String generateFileName(String tableName, Column[] columns) {
        String columnName = generateColumnNames(columns);
        return tableName + "_" + columnName + ".txt";
    }
}
