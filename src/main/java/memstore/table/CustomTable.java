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
    protected ByteBuffer rowsBuffer;
    protected TreeMap<Integer, PairForIndex> firstIndex;
    protected TreeMap<Pair, PairForIndex> secondIndex;

    // rowSum Cache
    ByteBuffer rowSums;

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
        this.rowsBuffer = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.rowSums = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            this.rowsBuffer.put(curRow);

            // Setup rowSum cache
            int curRowSum = 0;
            for (int colId = 0; colId < numCols; colId++) {
                curRowSum += getIntField(rowId, colId);
            }
            rowSums.putInt(curRowSum);

            int col0Val = getIntField(rowId, 0);
            sumCol0 += col0Val;
            // Build first index cache
            addFirstIndex(col0Val, rowId);

            // Build second index cache
            int col1Val = getIntField(rowId, 1);
            int col2Val = getIntField(rowId, 2);
            Pair col12Val = new Pair(col1Val, col2Val);
            addSecondIndex(col12Val, rowId);
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
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        rowsBuffer.putInt(offset, field);

        // Update rowSum cache
        int oldRowSum = rowSums.getInt(ByteFormat.FIELD_LEN * rowId);
        rowSums.putInt(ByteFormat.FIELD_LEN * rowId, oldRowSum - oldField + field);

        if (colId == 0) {
            // Delete and add reference
            deleteFirstIndex(oldField, rowId);
            addFirstIndex(field, rowId);
            sumCol0 += field - oldField;
        } else if (colId == 1 || colId == 2) {
            Pair col12Val;
            Pair updatedKeyPair;

            // Delete and add reference
            if (colId == 1) {
                int col2Val = getIntField(rowId, 2);
                col12Val = new Pair(oldField, col2Val);
                deleteSecondIndex(col12Val, rowId);
                updatedKeyPair = new Pair(field, col2Val);
            } else {
                int col1Val = getIntField(rowId, 1);
                col12Val = new Pair(col1Val, oldField);
                deleteSecondIndex(col12Val, rowId);
                updatedKeyPair = new Pair(col1Val, field);
            }
            addSecondIndex(updatedKeyPair, rowId);
        }

        // Need to update firstIndex colSum
        if (colId != 0) {
            int col0Val = getIntField(rowId, 0);
            PairForIndex pair = firstIndex.get(col0Val);
            int sumCol = pair.getSumCol();
            sumCol += field - oldField;
            pair.setSumCol(sumCol);
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
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2; (5,5) || [{1,1},{5,5}, {1,5}, {5,1}]
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;
        for (Map.Entry<Pair, PairForIndex> entry : secondIndex.entrySet()) {
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
            PairForIndex pair = entry.getValue();
            sum += pair.getSumCol();
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
        for (Map.Entry<Integer, PairForIndex> entry : firstIndex.descendingMap().entrySet()) {
            int col0Val = entry.getKey();
            if (col0Val <= threshold) {
                break;
            }
            sum += entry.getValue().getSumCol();
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
        for (Map.Entry<Integer, PairForIndex> entry : firstIndex.entrySet()) {
            int col0Val = entry.getKey();
            if (col0Val >= threshold) {
                break;
            }
            IntArrayList rowIds = entry.getValue().getRowIds();
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
        PairForIndex pair = firstIndex.getOrDefault(key, null);
        if (pair == null) {
            IntArrayList rowIds = new IntArrayList();
            rowIds.add(rowId);
            pair = new PairForIndex(getListSum(rowIds), rowIds);
            firstIndex.put(key, pair);
        } else {
            int sum = pair.getSumCol();
            IntArrayList rowIds = pair.getRowIds();
            rowIds.add(rowId);
            sum += rowSums.getInt(rowId * ByteFormat.FIELD_LEN);
            pair.setSumCol(sum);
        }
    }

    private void deleteFirstIndex(int key, int rowId) {
        PairForIndex pair = firstIndex.get(key);
        int sum = pair.getSumCol();
        IntArrayList rowIds = pair.getRowIds();
        rowIds.rem(rowId);
        if (rowIds.size() == 0) {
            firstIndex.remove(key);
        } else {
            sum -= rowSums.getInt(rowId * ByteFormat.FIELD_LEN);
            pair.setSumCol(sum);
        }
    }

    private void addSecondIndex(Pair keyPair, int rowId) {
        PairForIndex pair = secondIndex.getOrDefault(keyPair, null);
        if (pair == null) {
            IntArrayList rowIds = new IntArrayList();
            rowIds.add(rowId);
            pair = new PairForIndex(getCol0Sum(rowIds), rowIds);
            secondIndex.put(keyPair, pair);
        } else {
            int sum = pair.getSumCol();
            IntArrayList rowIds = pair.getRowIds();
            rowIds.add(rowId);
            sum += getIntField(rowId, 0);
            pair.setSumCol(sum);
        }
    }

    private void deleteSecondIndex(Pair keyPair, int rowId) {
        PairForIndex pair = secondIndex.get(keyPair);
        int sum = pair.getSumCol();
        IntArrayList rowIds = pair.getRowIds();
        rowIds.rem(rowId);
        if(rowIds.size() == 0) {
            secondIndex.remove(keyPair);
        } else {
            sum -= getIntField(rowId, 0);
            pair.setSumCol(sum);
        }
    }

    // get each row's col sum
    private int getListSum(IntArrayList list) {
        int sum = 0;
        for (int rowId : list) {
            sum += rowSums.getInt(rowId * ByteFormat.FIELD_LEN);
        }
        return sum;
    }

    // get each row's col
    private int getCol0Sum(IntArrayList list) {
        int sum = 0;
        for (int rowId : list) {
            sum += getIntField(rowId, 0);
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
        }
    }

    private static class PairForIndex {
        private int sumCol;
        private IntArrayList rowIds;

        public PairForIndex(int sumCol, IntArrayList rowIds) {
            this.sumCol = sumCol;
            this.rowIds = rowIds;
        }

        public int getSumCol() {
            return sumCol;
        }

        public void setSumCol(int sumCol23) {
            this.sumCol = sumCol23;
        }

        public IntArrayList getRowIds() {
            return rowIds;
        }

        public void setRowIds(IntArrayList rowIds) {
            this.rowIds = rowIds;
        }
    }
}
