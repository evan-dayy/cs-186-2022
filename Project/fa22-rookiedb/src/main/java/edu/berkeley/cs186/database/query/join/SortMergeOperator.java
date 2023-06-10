package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.*;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
              prepareRight(transaction, rightSource, rightColumnName),
              leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source isn't sorted, wraps the right source in a sort
     * operator. Otherwise, if it isn't materialized, wraps the right source in
     * a materialize operator. Otherwise, simply returns the right source. Note
     * that the right source must be materialized since we may need to backtrack
     * over it, unlike the left source.
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator implements Iterator<Record> {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();
            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }
            this.marked = false;
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            while (leftRecord != null) {
                if (rightRecord != null && !marked) {
                    // advance the lesser until get to a match
                    while (compare(leftRecord, rightRecord) < 0) {
                        if (leftIterator.hasNext()) {
                            this.leftRecord = leftIterator.next();
                        } else {
                            this.leftRecord = null;
                            return null;
                        }
                    }
                    while (compare(leftRecord, rightRecord) > 0) {
                        if (rightIterator.hasNext()) {
                            this.rightRecord = rightIterator.next();
                        } else {
                            this.rightRecord = null;
                            break;
                        }
                    }
                    // mark beginning of matching right records
                    this.rightIterator.markPrev();
                    this.marked = true;
                }

                if (rightRecord != null && compare(leftRecord, rightRecord) == 0) {
                    // there's a next right record, join it if there's a match
                    Record result = this.leftRecord.concat(rightRecord);
                    this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    return result;
                } else {
                    // there's no more matching right records, reset right and advance left
                    this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    this.rightIterator.reset();
                    this.marked = false;
                    this.rightRecord = this.rightIterator.next();
                }
            }
            // The left source was empty, nothing to fetch
            return null;
        }


        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {

            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
