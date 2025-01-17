package org.h2.mvstore.db;

import java.util.*;

import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.hasbithelper.FileHelper;
import org.h2.index.hasbithelper.HashBitObject;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.type.DataType;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.VersionedValue;

public final class HashBitIndex extends MVIndex<SearchRow, Value> {

    /**
     * The multi-value table.
     */
    private final MVTable mvTable;
    private final TransactionMap<SearchRow, Value> dataMap;
    private final int numberOfBuckets;
    private HashBitObject hashBitObject;

    public HashBitIndex(Database db, MVTable table, int id, String indexName,
                        IndexColumn[] columns, int uniqueColumnCount, IndexType indexType) {
        super(table, id, indexName, columns, uniqueColumnCount, indexType);

        if (uniqueColumnCount != 0) {
            throw DbException.getUnsupportedException(
                    "Cannot index unique columns in a hash-bit index");
        }
        if (columns.length != 1) {
            throw DbException.getUnsupportedException(
                    "Can only index one column in a hash-bit index");
        }

        IndexColumn col = columns[0];

        List<Integer> acceptedTypes = Arrays.asList(Value.CHAR, Value.VARCHAR, Value.VARCHAR_IGNORECASE);
        if (!acceptedTypes.contains(col.column.getType().getValueType())) {
            throw DbException.getUnsupportedException(
                    "Hash-bit index on non-character column, "
                            + col.column.getCreateSQL());
        }

        this.mvTable = table;
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        String mapName = "index." + getId();
        RowDataType keyType = getRowFactory().getRowDataType();
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(mapName, keyType, NullValueDataType.INSTANCE);
        dataMap.map.setVolatile(!table.isPersistData() || !indexType.isPersistent());
        if (!db.isStarting()) {
            dataMap.clear();
        }
        t.commit();
        if (!keyType.equals(dataMap.getKeyType())) {
            throw DbException.getInternalError(
                    "Incompatible key type, expected " + keyType + " but got "
                            + dataMap.getKeyType() + " for index " + indexName);
        }
        numberOfBuckets = indexType.getNumberOfHashbitBuckets() > 0 ?
                indexType.getNumberOfHashbitBuckets() : HashBitObject.DEFAULT_NUMBER_OF_BUCKETS;

        if (FileHelper.isFileExists(table.getName())) {
            this.hashBitObject = FileHelper.ReadObjectFromFile(table.getName()+ "_" +columns[0]+".txt");
        } else {
            this.hashBitObject = new HashBitObject(numberOfBuckets, table.getName(), columns[0].columnName);
        }
//        FileHelper.addNewHashObject(table.getName(), this.columns, numberOfBuckets);
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        throw DbException.getInternalError();
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        throw DbException.getInternalError();
    }

    private MVMap<SearchRow, Value> openMap(String mapName) {
        throw DbException.getInternalError();
    }

    @Override
    public void close(SessionLocal session) {
        // ok
    }

    @Override
    public void add(SessionLocal session, Row row) {

        MVPrimaryIndex pindex = this.mvTable.getPrimaryIndex();
        long index = row.getKey();

        Value[] values = row.getValueList();
//        String path = FileHelper.generateFileName(table.getName(), this.columns);
        Column column = this.columns[0];
        hashBitObject.add(values[column.getColumnId()].getString(), pindex.getIndexForKey(index));
//        HashBitObject obj = FileHelper.ReadObjectFromFile(path);
//        Column column = this.columns[0];
////
//        obj.add(values[column.getColumnId()].getString(), pindex.getIndexForKey(index));
//        FileHelper.WriteObjectToFile(path, obj);
    }

    private void checkUnique(boolean repeatableRead, TransactionMap<SearchRow,Value> map, SearchRow row,
                             long newKey) {
        throw DbException.getInternalError();
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        MVPrimaryIndex pindex = this.mvTable.getPrimaryIndex();
        long index = row.getKey();
        hashBitObject.remove(pindex.getIndexForKey(index), false);
//        String path = FileHelper.generateFileName(table.getName(), this.columns);
//        HashBitObject obj = FileHelper.ReadObjectFromFile(path);

        //TODO: This works only for column array with one column, Update that

//        obj.remove(pindex.getIndexForKey(index), false);
//        FileHelper.WriteObjectToFile(path, obj);
        System.out.println("After remove index:- " + index + ":- " + hashBitObject);
    }

    @Override
    public void update(SessionLocal session, Row oldRow, Row newRow) {
        Value[] newValues = newRow.getValueList();
        Value[] oldValues = oldRow.getValueList();
//        String path = FileHelper.generateFileName(table.getName(), this.columns);
//        HashBitObject obj = FileHelper.ReadObjectFromFile(path);
        long index = oldRow.getKey();
        Column column = this.columns[0];
        //TODO: This works only for column array with one column, Update that
//        System.out.println("====================================================================================================================");
//        System.out.println("Previous bitmap before update:- " + obj);
        hashBitObject.update(index, newValues[column.getColumnId()].getString(), oldValues[column.getColumnId()].getString());
//        FileHelper.WriteObjectToFile(path, obj);
        System.out.println("After update index:- " + index + ":- " + hashBitObject);
//        System.out.println("====================================================================================================================");
    }

    //TODO: Remove
    private boolean rowsAreEqual(SearchRow rowOne, SearchRow rowTwo) {
        throw DbException.getInternalError();
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {
//        return find(session, first, false, last);
        return null;
    }

//    private Cursor find(SessionLocal session, SearchRow first, boolean bigger, SearchRow last) {
//        SearchRow min = convertToKey(first, bigger);
//        SearchRow max = convertToKey(last, Boolean.TRUE);
//        return new MVStoreCursor(session, getTransactionMap(session).keyIterator(min, max), mvTable);
//    }

    private SearchRow convertToKey(SearchRow r, Boolean minMax) {
        throw DbException.getInternalError();
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    //TODO: Remove or Implement
    @Override
    public double getCost(SessionLocal session, int[] masks,
                          TableFilter[] filters, int filter, SortOrder sortOrder,
                          AllColumnsForPlan allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, false, allColumnsSet);
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    //TODO: Implement
    @Override
    public void remove(SessionLocal session) {
//        FileHelper.deleteFiles();
        this.hashBitObject.deleteFiles();
        System.out.println("Remove the Index File");
    }

    //TODO: Implement
    @Override
    public void truncate(SessionLocal session) {
//        FileHelper.addNewHashObject(table.getName(), this.columns, numberOfBuckets);
        hashBitObject.deleteFiles();
        hashBitObject = new HashBitObject(numberOfBuckets, table.getName(), columns[0].getName());
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(SessionLocal session, boolean first) {
//        System.out.println("Query Check - The method findFirstOrLast is called, Boolwan first :-  " + first);
//        TransactionMap.TMIterator<SearchRow, Value, SearchRow> iter = getTransactionMap(session).keyIterator(null, !first);
//        for (SearchRow key; (key = iter.fetchNext()) != null;) {
//            if (key.getValue(columnIds[0]) != ValueNull.INSTANCE) {
//                return new SingleRowCursor(mvTable.getRow(session, key.getKey()));
//            }
//        }
//        return new SingleRowCursor(null);
        return null;
    }

    @Override
    public boolean needRebuild() {
        try {
//            String path = FileHelper.generateFileName(table.getName(), this.columns);
//            HashBitObject obj = FileHelper.ReadObjectFromFile(path);
            return hashBitObject.getSize() == 0;
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(SessionLocal session) {
//        String path = FileHelper.generateFileName(table.getName(), this.columns);
//        HashBitObject obj = FileHelper.ReadObjectFromFile(path);
        return hashBitObject.getSize();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        try {
//            String path = FileHelper.generateFileName(table.getName(), this.columns);
//            HashBitObject obj = FileHelper.ReadObjectFromFile(path);
            return hashBitObject.getSize();
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public boolean canFindNext() {
        return true;
    }

    @Override
    public Cursor findNext(SessionLocal session, SearchRow higherThan, SearchRow last) {
//        return find(session, higherThan, true, last);
        return null;
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    //TODO: Remove
    public TransactionMap<SearchRow,Value> getTransactionMap(SessionLocal session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    //TODO: return null ? Remove
    @Override
    public MVMap<SearchRow, VersionedValue<Value>> getMVMap() {
        return dataMap.map;
    }

//    TODO: //REMOVE
    /**
     * A cursor.
     */
//    static final class MVStoreCursor implements Cursor {
//
//        private final SessionLocal             session;
//        private final TransactionMap.TMIterator<SearchRow, Value, SearchRow> it;
//        private final MVTable             mvTable;
//        private       SearchRow           current;
//        private       Row                 row;
//
//        MVStoreCursor(SessionLocal session, TransactionMap.TMIterator<SearchRow, Value, SearchRow> it, MVTable mvTable) {
//            this.session = session;
//            this.it = it;
//            this.mvTable = mvTable;
//        }
//
//        @Override
//        public Row get() {
//            if (row == null) {
//                SearchRow r = getSearchRow();
//                if (r != null) {
//                    row = mvTable.getRow(session, r.getKey());
//                }
//            }
//            return row;
//        }
//
//        @Override
//        public SearchRow getSearchRow() {
//            return current;
//        }
//
//        @Override
//        public boolean next() {
//            current = it.fetchNext();
//            row = null;
//            return current != null;
//        }
//
//        @Override
//        public boolean previous() {
//            throw DbException.getUnsupportedException("previous");
//        }
//    }

    public void rebuildIndex(SessionLocal session) {
        //TODO: Impplement this methos for all table. Currently, its implemented for MVTables
        Index scan = this.table.getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        Store store = session.getDatabase().getStore();

        int bufferSize = database.getMaxMemoryRows() / 2;
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = getName() + ':' + this.getName();
        ArrayList<String> bufferNames = Utils.newSmallArrayList();
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n, i++, total);
            if (buffer.size() >= bufferSize) {
                this.mvTable.sortRows(buffer, this.table.getPrimaryKey());
                String mapName = store.nextTemporaryMapName();
                this.addRowsToBuffer(buffer, mapName);
                bufferNames.add(mapName);
                buffer.clear();
            }
            remaining--;
        }
        //TODO: We need table order, not the index sorted order
        this.mvTable.sortRows(buffer, this.table.getPrimaryKey());
        if (!bufferNames.isEmpty()) {
            //TODO: Need to handle this by preventing updating the datamap
            String mapName = store.nextTemporaryMapName();
            this.addRowsToBuffer(buffer, mapName);
            bufferNames.add(mapName);
            buffer.clear();
            this.addBufferedRows(bufferNames);
        } else {
            this.mvTable.addRowsToIndexAndSortByPrimaryKey(session, buffer, this);
        }
        if (remaining != 0) {
            throw DbException.getInternalError("rowcount remaining=" + remaining + ' ' + getName());
        }
//        HashBitObject hashBitObject = FileHelper.ReadObjectFromFile(FileHelper.generateFileName(table.getName(), this.columns));
//        System.out.println("===============================================================================================");
//        System.out.println("Rebuilt hashbit indexes of Table - " + table.getName() + " and Column - " + this.columns[0].getName());
//        System.out.println(hashBitObject.toString());
//        System.out.println("===============================================================================================");
    }

    public ArrayList<Boolean> getBitMapArray(String value){
        return hashBitObject.getBitmapArray(value);
    }

    //TODO: Remove
    public void addSchemaObject() {
        
    }
}