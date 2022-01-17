package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;
import memstore.table.tree.BTree;

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
    protected ByteBuffer rowsBuffer;

    protected BTree<Integer, Map.Entry<Integer, IntArrayList>> firstIndex;
    protected BTree<Pair, IntArrayList> secondIndex;

    // rowSum Cache
    protected ByteBuffer rowSums;

    public CustomTable() {
        firstIndex = new BTree<>();
        secondIndex = new BTree<>();
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
        this.rowsBuffer = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.rowSums = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            this.rowsBuffer.put(curRow);

            int col0Val = getIntField(rowId, 0);
            sumCol0 += col0Val;
            // Build first index cache
            addFirstIndex(col0Val, rowId);

            // Build second index cache
            int col1Val = getIntField(rowId, 1);
            int col2Val = getIntField(rowId, 2);
            Pair pair = new Pair(col1Val, col2Val);
            addSecondIndex(pair, rowId);

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
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return rowsBuffer.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int oldField = getIntField(rowId, colId);

        // Update reference for indexes
        if (colId == 0) {
            deleteFirstIndex(oldField, rowId);
            addFirstIndex(field, rowId);
            sumCol0 += field - oldField;
        } else if (colId == 1) {
            int col2Val = getIntField(rowId, 2);
            Pair pair = new Pair(oldField, col2Val);
            deleteSecondIndex(pair, rowId);
            addSecondIndex(pair, rowId);
        } else if (colId == 2) {
            int col1Val = getIntField(rowId, 1);
            Pair pair = new Pair(col1Val, oldField);
            deleteSecondIndex(pair, rowId);
            addSecondIndex(pair, rowId);
        }
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        rowsBuffer.putInt(offset, field);

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
//        Pair pair = new Pair(threshold1, -1);
//        for(IntArrayList rowIds : secondIndex.lesserQuery(pair)) {
//            for (int rowId : rowIds) {
//                int col1Val = getIntField(rowId, 1);
//                if (col1Val <= threshold1) {
//                    break;
//                }
//                int col2Val = getIntField(rowId, 2);
//                if (col2Val >= threshold2) {
//                    break;
//                }
//                sum += getIntField(rowId, 0);
//            }
//        }
        Pair pair = new Pair(threshold1 - 1, -1);
        for(IntArrayList rowIds : secondIndex.lesserQuery(pair)) {
            for (int rowId : rowIds) {
                int col1Val = getIntField(rowId, 1);
                if (col1Val <= threshold1) {
                    break;
                }
                int col2Val = getIntField(rowId, 2);
                if (col2Val >= threshold2) {
                    continue;
                }
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
        for (Map.Entry<Integer, IntArrayList> entry : firstIndex.greaterQuery(threshold)) {
            for (int rowId : entry.getValue()) {
                sum += rowSums.getInt(rowId * ByteFormat.FIELD_LEN);
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
        for (Map.Entry<Integer, IntArrayList> entry : firstIndex.lesserQuery(threshold)) {
            for (int rowId : entry.getValue()) {
                ++count;
                int col2Val = getIntField(rowId, 2);
                int col3Val = getIntField(rowId, 3);
                putIntField(rowId, 3, col2Val + col3Val);
            }
        }
        return count;
    }

    private void addFirstIndex(int key, int rowId) {
        Map.Entry<Integer, IntArrayList> entry = firstIndex.get(key);
        if (entry == null) {
            IntArrayList rowIds = new IntArrayList();
            rowIds.add(rowId);
            entry = new AbstractMap.SimpleEntry<>(getSum(rowIds), rowIds);
            firstIndex.put(key, entry);
        } else {
            IntArrayList rowIds = entry.getValue();
            rowIds.add(rowId);
        }
    }

    private void deleteFirstIndex(int key, int rowId) {
        Map.Entry<Integer, IntArrayList> entry = firstIndex.get(key);
        IntArrayList rowIds = entry.getValue();
        rowIds.rem(rowId);
    }

    private void addSecondIndex(Pair pair, int rowId) {
        IntArrayList rowIds = secondIndex.get(pair);
        if (rowIds == null) {
            rowIds = new IntArrayList();
            rowIds.add(rowId);
            secondIndex.put(pair, rowIds);
        } else {
            rowIds.add(rowId);
        }
    }

    private void deleteSecondIndex(Pair pair, int rowId) {
        IntArrayList rowIds = secondIndex.get(pair);
        rowIds.rem(rowId);
    }

    private int getSum(IntArrayList list) {
        int sum = 0;
        for (int num : list) {
            sum += num;
        }
        return sum;
    }

    /**
     * To maintain a following order in the index cache
     *      {a, b}, {c, d}, {e, f}
     *      a > c, b < d; c > e, d < f
     *
     * priority of col1Val > priority of col2Val
     */
    private static class Pair implements Comparable<Pair> {
        private final int col1Val;
        private final int col2Val;

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
//            return (10000 * p.getCol1Val() + p.getCol2Val()) - (10000 * col1Val + col2Val);
//            if (col1Val == p.getCol1Val()) {
//                return p.getCol2Val() - col2Val;
//            }
//            return p.getCol1Val() - col1Val;
        }
    }
}
