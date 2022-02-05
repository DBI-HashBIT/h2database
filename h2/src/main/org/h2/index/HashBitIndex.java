package org.h2.index;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;

public class HashBitIndex extends Index {

    /**
     * Initialize the index.
     *
     * @param newTable          the table
     * @param id                the object id
     * @param name              the index name
     * @param newIndexColumns   the columns that are indexed or null if this is
     *                          not yet known
     * @param uniqueColumnCount count of unique columns
     * @param newIndexType      the index type
     */
    protected HashBitIndex(Table newTable, int id, String name, IndexColumn[] newIndexColumns, int uniqueColumnCount, IndexType newIndexType) {
        super(newTable, id, name, newIndexColumns, uniqueColumnCount, newIndexType);
    }

    @Override
    public void close(SessionLocal session) {

    }

    @Override
    public void add(SessionLocal session, Row row) {

    }

    @Override
    public void remove(SessionLocal session, Row row) {

    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {
        return null;
    }

    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, AllColumnsForPlan allColumnsSet) {
        return 0;
    }

    @Override
    public void remove(SessionLocal session) {

    }

    @Override
    public void truncate(SessionLocal session) {

    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return 0;
    }
}