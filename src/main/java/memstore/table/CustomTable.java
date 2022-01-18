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
    protected TreeMap<Pair, Integer> secondIndex;

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
            Pair col12Val = new Pair(col1Val, col2Val);
            addSecondIndex(col12Val, col0Val);

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
        final int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columnsBuffer.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        final int oldField = getIntField(rowId, colId);
        if (colId == 0) {
            // Delete and add reference
            deleteFirstIndex(oldField, rowId);
            addFirstIndex(field, rowId);
            sumCol0 += field - oldField;

            // update secondIndex cache
            final int col1Val = getIntField(rowId, 1);
            final int col2Val = getIntField(rowId, 2);
            Pair pair = new Pair(col1Val, col2Val);
            deleteSecondIndex(pair, oldField);
            addSecondIndex(pair, field);
        } else if (colId == 1 || colId == 2) {
            Pair col12Val;
            int col0Val = getIntField(rowId, 0);

            // Delete and add reference
            if (colId == 1) {
                int col2Val = getIntField(rowId, 2);
                col12Val = new Pair(oldField, col2Val);
                deleteSecondIndex(col12Val, col0Val);
                col12Val.setCol1Val(field);
                col12Val.setCol2Val(col2Val);
            } else {
                int col1Val = getIntField(rowId, 1);
                col12Val = new Pair(col1Val, oldField);
                deleteSecondIndex(col12Val, col0Val);
                col12Val.setCol1Val(col1Val);
                col12Val.setCol2Val(field);
            }
            addSecondIndex(col12Val, col0Val);
        }
        final int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        columnsBuffer.putInt(offset, field);

        // Update rowSum cache
        final int oldRowSum = rowSums.getInt(ByteFormat.FIELD_LEN * rowId);
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
        for (Map.Entry<Pair, Integer> entry : secondIndex.entrySet()) {
            Pair keyPair = entry.getKey();
            // check the threshold
            int col1Val = keyPair.getCol1Val();
            if (col1Val <= threshold1) {
                break;
            }
            int col2Val = keyPair.getCol2Val();
            if (col2Val >= threshold2) {
                continue;
            }
            int rangeCol0Sum = entry.getValue();
            sum += rangeCol0Sum;
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
        for (Map.Entry<Integer, IntArrayList> entry : firstIndex.descendingMap().entrySet()) {
            int col0Val = entry.getKey();
            if (col0Val <= threshold) {
                break;
            }
            IntArrayList rowIds = entry.getValue();
            for (int i = 0; i < rowIds.size(); i++) {
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
        for (Map.Entry<Integer, IntArrayList> entry : firstIndex.entrySet()) {
            int col0Val = entry.getKey();
            if (col0Val >= threshold) {
                break;
            }
            IntArrayList rowIds = entry.getValue();
            for (int i = 0; i < rowIds.size(); i++) {
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

    private void addSecondIndex(Pair keyPair, int Col0Val) {
        final int rangeCol0Sum = secondIndex.getOrDefault(keyPair, 0);
        secondIndex.put(keyPair, rangeCol0Sum + Col0Val);
    }

    private void deleteSecondIndex(Pair keyPair, int Col0Val) {
        final int rangeCol0Sum = secondIndex.getOrDefault(keyPair, 0);
        secondIndex.put(keyPair, rangeCol0Sum - Col0Val);
    }

    /**
     * To maintain a following order in the index cache
     *      {a, b}, {c, d}, {e, f}
     *      a > c, b < d; c > e, d < f
     *
     * priority of col1Val > priority of col2Val
     */
    private static class Pair implements Comparable<Pair> {
        private int col1Val;
        private int col2Val;

        public Pair(int col1Val, int col2Val) {
            this.col1Val = col1Val;
            this.col2Val = col2Val;
        }

        public int getCol1Val() {
            return col1Val;
        }

        public int getCol2Val() {
            return col2Val;
        }

        @Override
        public int compareTo(Pair p) {
            if (col1Val == p.getCol1Val()) {
                return col2Val - p.getCol2Val();
            }
            return p.getCol1Val() - col1Val;
        }

        public void setCol1Val(int col1Val) {
            this.col1Val = col1Val;
        }

        public void setCol2Val(int col2Val) {
            this.col2Val = col2Val;
        }
    }
}
