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
import org.h2.index.SingleRowCursor;
import org.h2.index.hasbithelper.FileHelper;
import org.h2.index.hasbithelper.HashBitObject;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.tx.TransactionMap.TMIterator;
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

        FileHelper.addNewHashObject(table.getName(), this.columns, numberOfBuckets);
    }

    //TODO: Throw an error
    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        MVMap<SearchRow, Value> map = openMap(bufferName);
        for (Row row : rows) {
            SearchRow r = getRowFactory().createRow();
            r.copyFrom(row);
            map.append(r, ValueNull.INSTANCE);
        }
    }

    //TODO: Remove this part
    private static final class Source {

        private final Iterator<SearchRow> iterator;

        SearchRow currentRowData;

        public Source(Iterator<SearchRow> iterator) {
            assert iterator.hasNext();
            this.iterator = iterator;
            this.currentRowData = iterator.next();
        }

        public boolean hasNext() {
            boolean result = iterator.hasNext();
            if (result) {
                currentRowData = iterator.next();
            }
            return result;
        }

        public SearchRow next() {
            return currentRowData;
        }

        static final class Comparator implements java.util.Comparator<Source> {

            private final DataType<SearchRow> type;

            public Comparator(DataType<SearchRow> type) {
                this.type = type;
            }

            @Override
            public int compare(Source one, Source two) {
                return type.compare(one.currentRowData, two.currentRowData);
            }
        }
    }

    //TODO: Throw an error
    @Override
    public void addBufferedRows(List<String> bufferNames) {
        int buffersCount = bufferNames.size();
        Queue<Source> queue = new PriorityQueue<>(buffersCount,
                new Source.Comparator(getRowFactory().getRowDataType()));
        for (String bufferName : bufferNames) {
            Iterator<SearchRow> iter = openMap(bufferName).keyIterator(null);
            if (iter.hasNext()) {
                queue.offer(new Source(iter));
            }
        }

        try {
            while (!queue.isEmpty()) {
                Source s = queue.poll();
                SearchRow row = s.next();

                if (uniqueColumnColumn > 0 && !mayHaveNullDuplicates(row)) {
                    checkUnique(false, dataMap, row, Long.MIN_VALUE);
                }

                dataMap.putCommitted(row, ValueNull.INSTANCE);

                if (s.hasNext()) {
                    queue.offer(s);
                }
            }
        } finally {
            MVStore mvStore = database.getStore().getMvStore();
            for (String tempMapName : bufferNames) {
                mvStore.removeMap(tempMapName);
            }
        }
    }

    //TODO: Remove this method
    private MVMap<SearchRow, Value> openMap(String mapName) {
        RowDataType keyType = getRowFactory().getRowDataType();
        MVMap.Builder<SearchRow, Value> builder = new MVMap.Builder<SearchRow, Value>()
                .singleWriter()
                .keyType(keyType)
                .valueType(NullValueDataType.INSTANCE);
        MVMap<SearchRow, Value> map = database.getStore().getMvStore()
                .openMap(mapName, builder);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.getInternalError(
                    "Incompatible key type, expected " + keyType + " but got "
                            + map.getKeyType() + " for map " + mapName);
        }
        return map;
    }

    @Override
    public void close(SessionLocal session) {
        // ok
    }

    @Override
    public void add(SessionLocal session, Row row) {
        Value[] values = row.getValueList();
        String path = FileHelper.generateFileName(table.getName(), this.columns);
        HashBitObject obj = FileHelper.ReadObjectFromFile(path);
        Column column = this.columns[0];
        //TODO: This works only for column array with one column, Update that
//        System.out.println("====================================================================================================================");
//        System.out.println("Previous bitmap:- " + obj);
        obj.add(values[column.getColumnId()].getString(), row.getKey());
        FileHelper.WriteObjectToFile(path, obj);
//        System.out.println("After add value:- " + values[column.getColumnId()].getString() + ":- " + FileHelper.ReadObjectFromFile(path));
//        System.out.println("====================================================================================================================");
    }

    //TODO: Remove
    private void checkUnique(boolean repeatableRead, TransactionMap<SearchRow,Value> map, SearchRow row,
                             long newKey) {
        RowFactory uniqueRowFactory = getUniqueRowFactory();
        SearchRow from = uniqueRowFactory.createRow();
        from.copyFrom(row);
        from.setKey(Long.MIN_VALUE);
        SearchRow to = uniqueRowFactory.createRow();
        to.copyFrom(row);
        to.setKey(Long.MAX_VALUE);
        if (repeatableRead) {
            // In order to guarantee repeatable reads, snapshot taken at the beginning of the statement or transaction
            // need to be checked additionally, because existence of the key should be accounted for,
            // even if since then, it was already deleted by another (possibly committed) transaction.
            TransactionMap.TMIterator<SearchRow, Value, SearchRow> it = map.keyIterator(from, to);
            for (SearchRow k; (k = it.fetchNext()) != null;) {
                if (newKey != k.getKey() && !map.isDeletedByCurrentTransaction(k)) {
                    throw getDuplicateKeyException(k.toString());
                }
            }
        }
        TransactionMap.TMIterator<SearchRow, Value, SearchRow> it = map.keyIteratorUncommitted(from, to);
        for (SearchRow k; (k = it.fetchNext()) != null;) {
            if (newKey != k.getKey()) {
                if (map.getImmediate(k) != null) {
                    // committed
                    throw getDuplicateKeyException(k.toString());
                }
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
            }
        }
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        String path = FileHelper.generateFileName(table.getName(), this.columns);
        HashBitObject obj = FileHelper.ReadObjectFromFile(path);
        long index = row.getKey();
        //TODO: This works only for column array with one column, Update that
//        System.out.println("====================================================================================================================");
//        System.out.println("Previous bitmap before remove:- " + obj);
        obj.remove(index, false);
        FileHelper.WriteObjectToFile(path, obj);
//        System.out.println("After remove index:- " + index + ":- " + FileHelper.ReadObjectFromFile(path));
//        System.out.println("====================================================================================================================");
    }

    @Override
    public void update(SessionLocal session, Row oldRow, Row newRow) {
        Value[] newValues = newRow.getValueList();
        Value[] oldValues = newRow.getValueList();
        String path = FileHelper.generateFileName(table.getName(), this.columns);
        HashBitObject obj = FileHelper.ReadObjectFromFile(path);
        long index = oldRow.getKey();
        Column column = this.columns[0];
        //TODO: This works only for column array with one column, Update that
//        System.out.println("====================================================================================================================");
//        System.out.println("Previous bitmap before update:- " + obj);
        obj.update(index, newValues[column.getColumnId()].getString(), oldValues[column.getColumnId()].getString());
        FileHelper.WriteObjectToFile(path, obj);
        System.out.println("After update index:- " + index + ":- " + FileHelper.ReadObjectFromFile(path));
//        System.out.println("====================================================================================================================");
    }

    //TODO: Remove
    private boolean rowsAreEqual(SearchRow rowOne, SearchRow rowTwo) {
        if (rowOne == rowTwo) {
            return true;
        }
        for (int index : columnIds) {
            Value v1 = rowOne.getValue(index);
            Value v2 = rowTwo.getValue(index);
            if (!Objects.equals(v1, v2)) {
                return false;
            }
        }
        return rowOne.getKey() == rowTwo.getKey();
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

    //TODO: Remove
    private SearchRow convertToKey(SearchRow r, Boolean minMax) {
        if (r == null) {
            return null;
        }

        SearchRow row = getRowFactory().createRow();
        row.copyFrom(r);
        if (minMax != null) {
            row.setKey(minMax ? Long.MAX_VALUE : Long.MIN_VALUE);
        }
        return row;
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
        //TODO: Remove the index file
        System.out.println("Remove the Index File");
    }

    //TODO: Implement
    @Override
    public void truncate(SessionLocal session) {
        FileHelper.addNewHashObject(table.getName(), this.columns, numberOfBuckets);
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
            String path = FileHelper.generateFileName(table.getName(), this.columns);
            HashBitObject obj = FileHelper.ReadObjectFromFile(path);
            return obj.getSize() == 0;
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(SessionLocal session) {
        String path = FileHelper.generateFileName(table.getName(), this.columns);
        HashBitObject obj = FileHelper.ReadObjectFromFile(path);
        return obj.getSize();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        try {
            String path = FileHelper.generateFileName(table.getName(), this.columns);
            HashBitObject obj = FileHelper.ReadObjectFromFile(path);
            return obj.getSize();
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
        HashBitObject hashBitObject = FileHelper.ReadObjectFromFile(FileHelper.generateFileName(table.getName(), this.columns));
//        System.out.println("===============================================================================================");
//        System.out.println("Rebuilt hashbit indexes of Table - " + table.getName() + " and Column - " + this.columns[0].getName());
//        System.out.println(hashBitObject.toString());
//        System.out.println("===============================================================================================");
    }

    //TODO: Remove
    public void addSchemaObject() {
        
    }
}