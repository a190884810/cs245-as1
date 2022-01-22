package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {
    protected int numRows;
    protected int numCols;
    protected int sumCol0;
    protected ByteBuffer columnsBuffer;
    protected TreeMap<Integer, IntArrayList> firstIndex;
    protected TreeMap<Integer, TreeMap<Integer, Integer>> secondIndex;

    // rowSum Cache
    protected ByteBuffer rowSums;

    public CustomTable() {
        firstIndex = new TreeMap<>();
        secondIndex = new TreeMap<>();
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
        this.columnsBuffer = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.rowSums = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
                this.columnsBuffer.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }

        for (int rowId = 0; rowId < numRows; rowId++) {
            int col0Val = getIntField(rowId, 0);
            sumCol0 += col0Val;
            // Build first index cache
            addFirstIndex(col0Val, rowId);

            // Build second index cache
            int col1Val = getIntField(rowId, 1);
            int col2Val = getIntField(rowId, 2);
            addSecondIndex(col1Val, col2Val, col0Val);

            // Setup rowSum cache
            int curRowSum = 0;
            for (int colId = 0; colId < numCols; colId++) {
                curRowSum += getIntField(rowId, colId);
            }
            rowSums.putInt(curRowSum);
        }
        rowSums.rewind();
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columnsBuffer.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int oldField = getIntField(rowId, colId);
        if (colId == 0) {
            // Delete and add reference
            deleteFirstIndex(oldField, rowId);
            addFirstIndex(field, rowId);
            sumCol0 += field - oldField;

            // update secondIndex cache
            int col1Val = getIntField(rowId, 1);
            int col2Val = getIntField(rowId, 2);
            deleteSecondIndex(col1Val, col2Val, oldField);
            addSecondIndex(col1Val, col2Val, field);
        } else if (colId == 1 || colId == 2) {
            int col0Val = getIntField(rowId, 0);
            // Delete and add reference
            if (colId == 1) {
                int col2Val = getIntField(rowId, 2);
                deleteSecondIndex(oldField, col2Val, col0Val);
                addSecondIndex(field, col2Val, col0Val);
            } else {
                int col1Val = getIntField(rowId, 1);
                deleteSecondIndex(col1Val, oldField, col0Val);
                addSecondIndex(col1Val, field, col0Val);
            }
        }
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        columnsBuffer.putInt(offset, field);

        // Update rowSum cache
        int oldRowSum = rowSums.getInt(ByteFormat.FIELD_LEN * rowId);
        rowSums.putInt(ByteFormat.FIELD_LEN * rowId, oldRowSum - oldField + field);
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
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2; (5,5) || [{1,1},{5,5}, {1,5}, {5,1}]
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;
        for (TreeMap<Integer, Integer> subIndex : secondIndex.tailMap(threshold1, false).values()) {
            for (Integer col0Val : subIndex.headMap(threshold2, false).values()) {
                sum += col0Val;
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
        for (IntArrayList rowIds : firstIndex.tailMap(threshold, false).values()) {
            int size = rowIds.size();
            for (int i = 0; i < size; i++) {
                sum += rowSums.getInt(rowIds.getInt(i) * ByteFormat.FIELD_LEN);
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
        for (IntArrayList rowIds : firstIndex.headMap(threshold, false).values()) {
            int size = rowIds.size();
            for (int i = 0; i < size; i++) {
                ++count;
                int rowId = rowIds.getInt(i);
                int col2Val = getIntField(rowId, 2);
                int col3Val = getIntField(rowId, 3);
                putIntField(rowId, 3, col2Val + col3Val);
            }
        }
        return count;
    }

    private void addFirstIndex(int key, int rowId) {
        IntArrayList rowIds = firstIndex.getOrDefault(key, null);
        if (rowIds == null) {
            rowIds = new IntArrayList();
            firstIndex.put(key, rowIds);
        }
        rowIds.add(rowId);
    }

    private void deleteFirstIndex(int key, int rowId) {
        IntArrayList rowIds = firstIndex.get(key);
        rowIds.rem(rowId);
        if (rowIds.size() == 0) {
            firstIndex.remove(key);
        }
    }

    private void addSecondIndex(int key1, int key2, int col0Val) {
        TreeMap<Integer, Integer> subIndex = secondIndex.getOrDefault(key1, null);
        if (subIndex == null) {
            subIndex = new TreeMap<>();
            secondIndex.put(key1, subIndex);
        }
        subIndex.put(key2, subIndex.getOrDefault(key2, 0) + col0Val);
    }

    private void deleteSecondIndex(int key1, int key2, int col0Val) {
        TreeMap<Integer, Integer> subIndex = secondIndex.get(key1);
        subIndex.put(key2, subIndex.get(key2) - col0Val);
    }
}
