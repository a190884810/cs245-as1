package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    int sumCol0;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private final int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        this.numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.index = new TreeMap<>();

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int field = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                int offset = (rowId * numCols + colId) * ByteFormat.FIELD_LEN;
                this.rows.putInt(offset, field);
                if (colId == indexColumn) {
                    if (!this.index.containsKey(field)) {
                        IntArrayList rowIds = new IntArrayList();
                        rowIds.add(rowId);
                        this.index.put(field, rowIds);
                    } else {
                        this.index.get(field).add(rowId);
                    }

                }
            }

            int indexColVal = getIntField(rowId, 0);
            sumCol0 += indexColVal;
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = (rowId * numCols + colId) * ByteFormat.FIELD_LEN;
        return rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int oldField = getIntField(rowId, colId);
        if(oldField == field) {
            return;
        }
        int offset = (rowId * numCols + colId) * ByteFormat.FIELD_LEN;
        rows.putInt(offset, field);

        // Update index tree
        if (colId == indexColumn) {
            IntArrayList rowIds = this.index.get(oldField);
            rowIds.rem(rowId);
            if (rowIds.size() == 0) {
                this.index.remove(oldField);
            }
            if (!this.index.containsKey(field)) {
                this.index.put(field, new IntArrayList());
            }
            this.index.get(field).add(rowId);
        }
        if (colId == 0) {
            sumCol0 += field - oldField;
        }
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        return sumCol0;
    }

    /**
     * Check whether current row fits the threshold
     * @param rowId Current row id
     * @param colId current col id
     * @param threshold Either threshold1 or threshold2 depends on the value of {@code colId}
     * @return true if curernt row fits the threshold.
     */
    private boolean isValid(int rowId, int colId, int threshold) {
        int field = getIntField(rowId, colId);
        return colId == 1 ? field > threshold : field < threshold;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;

        // Apply index to improve performance
        if (indexColumn == 1 || indexColumn == 2) {
            for (Integer key : index.keySet()) {
                if(indexColumn == 2 && key >= threshold2) { // No need to continue the rest of the loop
                    break;
                }
                if (indexColumn == 1 && key > threshold1 || indexColumn == 2) {
                    IntArrayList validRowIds = index.get(key);
                    for (Integer rowId : validRowIds) {
                        if (isValid(rowId, 3 - indexColumn, indexColumn == 1 ? threshold2 : threshold1)) {
                            sum += getIntField(rowId, 0);
                        }
                    }
                }
            }
            return sum;
        }

        // Index can't be used
        for (int rowId = 0; rowId < numRows; rowId++) {
            int col1Val = getIntField(rowId, 1);
            int col2Val = getIntField(rowId, 2);
            if (col1Val > threshold1 && col2Val < threshold2) {
                sum += getIntField(rowId, 0);
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;

        // Apply index to improve performance
        if (indexColumn == 0) {
            for (Integer key : index.descendingKeySet()) {
                if(key <= threshold) { // No need to continue the rest of the loop
                    break;
                }
                IntArrayList validRowIds = index.get(key);
                for (Integer rowId : validRowIds) {
                    sum += key;
                    for (int colId = 1; colId < numCols; colId++) {
                        sum += getIntField(rowId, colId);
                    }
                }
            }
            return sum;
        }

        // Index can't be used
        for (int rowId = 0; rowId < numRows; rowId++) {
            int col0Val = getIntField(rowId, 0);
            if (col0Val > threshold) {
                sum += col0Val;
                for (int colId = 1; colId < numCols; colId++) {
                    sum += getIntField(rowId, colId);
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int count = 0;

        // Apply index to improve performance
        if (indexColumn == 0) {
            IntArrayList validRowIds = new IntArrayList();
            for (Integer key : index.keySet()) {
                if (key >= threshold) { // No need to continue the rest of the loop
                    break;
                }
                validRowIds.addAll(index.get(key));
            }
            for (Integer rowId : validRowIds) {
                int col3Val = getIntField(rowId, 3);
                int col2Val = getIntField(rowId, 2);
                putIntField(rowId, 3, col3Val + col2Val);
                ++count;
            }
            return count;
        }

        // Index can't be used
        for (int rowId = 0; rowId < numRows; rowId++) {
            int col0Val = getIntField(rowId, 0);
            if (col0Val < threshold) {
                int col3Val = getIntField(rowId, 3);
                int col2Val = getIntField(rowId, 2);
                putIntField(rowId, 3, col3Val + col2Val);
                ++count;
            }
        }
        return count;
    }
}
