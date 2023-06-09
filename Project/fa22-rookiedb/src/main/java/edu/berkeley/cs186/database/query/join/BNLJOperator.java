package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();
            this.rightSourceIterator = getRightSource().backtrackingIterator();
            // marking the start position
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();
            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            // need to check whether there are still has pages
            if (!leftSourceIterator.hasNext()) {
                leftRecord = null;
                return;
            }
            // set up the next block
            this.leftBlockIterator = getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            this.leftBlockIterator.markNext();
            this.leftRecord = this.leftBlockIterator.next();
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            // need to check whether there are still has pages
            if (!rightSourceIterator.hasNext()) return;
            this.rightPageIterator = getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
            this.rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            if (leftRecord == null) return null;
            // go through every element in a block and a page
            Record temp = fetchNextRecordWithinABlockAndPage(leftBlockIterator, rightPageIterator);
            // if temp has a value, then there is a match
            if (temp != null) return temp;
            // else there is no match for current block page
            // there are two options, either moving to the next page if there is a next right page
            if (rightSourceIterator.hasNext()) {
                fetchNextRightPage();
                leftBlockIterator.reset();
                // reset the location of leftRecord
                this.leftRecord = this.leftBlockIterator.next();
                return fetchNextRecord();
            }
            // other option is there we are reaching the last right page,
            rightSourceIterator.reset();
            fetchNextRightPage();
            fetchNextLeftBlock();
            return fetchNextRecord();
        }

        private Record fetchNextRecordWithinABlockAndPage(BacktrackingIterator<Record> leftBlock,
                                                          BacktrackingIterator<Record> rightPage) {
            while(true) {
                if (rightPage.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightPage.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftBlock.hasNext()){
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    this.leftRecord = leftBlock.next();
                    rightPage.reset();
                } else {
                    // if you're here then there are no more records to fetch
                    return null;
                }
            }
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
    }
}
