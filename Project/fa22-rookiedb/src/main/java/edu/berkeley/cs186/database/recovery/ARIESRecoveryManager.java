package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 * - ARIES Algorithm for recovery is a non-force & steal recovery Algorithm
 * - indicating the buffer manager may steal dirty page
 * - and the changes are flushed to disk after commit.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number. One of Functional Programming
    // call by using newTransaction.apply(tranNum) -> return transaction
    private Function<Long, Transaction> newTransaction;
    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    /**
     * two important properties:
     *  1. the transaction itself
     *  2. the last LSN (previous LSN Operating on this transaction)
     */
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     * The transaction should be added to the transaction table.
     * @param transaction new transaction
     * @synchronized only one thread can access at one time
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     * @param transNum transaction being committed
     * @return LSN of the commit record
     * @process append the record -> flush to the disk -> update transaction table (LSN)
     */
    @Override
    public long commit(long transNum) {
        // step 1: append the record to the log
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new CommitTransactionLogRecord(transNum, prevLSN));
        // step 2: Flush everything up to the LSN
        logManager.flushToLSN(LSN);
        // step 3: update the current transaction table
        transactionEntry.lastLSN = LSN;
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     * @process append the record -> update transaction table (LSN)
     */
    @Override
    public long abort(long transNum) {
        // step 1: append the record to the log
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, prevLSN));
        // step 2: update the current transaction table
        transactionEntry.lastLSN = LSN;
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     * @param transNum transaction to end
     * @return LSN of the end record
     * @process end of what? -> aborting transaction -> roll back and undo transaction
     *           |                                         |
     *           committing transaction -> append the record -> update transaction table (remove)
     */
    @Override
    public long end(long transNum) {
        // step 1: judge the current transaction status
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        // step 1: If aborting
        if (transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING) rollbackToLSN(transNum, 0);
        // step 2: append record
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, prevLSN));
        // step 3: update the transaction table
        transactionEntry.lastLSN = LSN;
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);
        return LSN;
    }

    /**
     * Helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        // starting from the last record
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // if the record is CLR, indicating it have undone, we can get next LSN directly
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // start from current LSN and go back search to undo
        while (currentLSN > LSN) {
            LogRecord current = logManager.fetchLogRecord(currentLSN);
            if (current.isUndoable()) {
                // writing ahead log
                LogRecord CLR = current.undo(lastRecordLSN); // does not perform undo, just create a record
                lastRecordLSN = logManager.appendToLog(CLR); // appending
                CLR.redo(this, diskSpaceManager, bufferManager); // undo performance
            }
            currentLSN = current.getUndoNextLSN().isPresent() ?
                    current.getUndoNextLSN().get() : current.getPrevLSN().get();
        }
        // update transaction table
        transactionEntry.lastLSN = lastRecordLSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        // ensure they are updating with same data type
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // append the record
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after));
        // Update transaction table and dirty page table, dirty page table keep the first update LSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     * @process append the record -> flush to disk -> update the transaction table
     * - after this operation, the disk manager know that the transaction are moving
     * - to another another partition to work
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        // append the record
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new AllocPartLogRecord(transNum, partNum, prevLSN));
        // Flush log
        logManager.flushToLSN(LSN);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     * @process same as the previous allocation, however, it free one partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        // append the record
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new FreePartLogRecord(transNum, partNum, prevLSN));
        // Flush log
        logManager.flushToLSN(LSN);
        // Update the transaction table
        transactionEntry.lastLSN = LSN;
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        // append the record
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new AllocPageLogRecord(transNum, pageNum, prevLSN));
        // Flush log
        logManager.flushToLSN(LSN);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     * @process if the page is already free, then we can remove it from the dirty page
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        // append the record first
        long prevLSN = transactionEntry.lastLSN;
        long LSN = logManager.appendToLog(new FreePageLogRecord(transNum, pageNum, prevLSN));
        // Flush log
        logManager.flushToLSN(LSN);
        // Update lastLSN of TXN  and also update the dirty page table
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     * @process calling the addSavePoint method directly if the entry exist
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        long savepointLSN = transactionEntry.getSavepoint(name);
        // directly call the rollback method, everything after and equal the savepoint should be undone
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     * First, a begin checkpoint record should be written.
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // WAL: Writing ahead the starting log record
        long beginLSN = logManager.appendToLog(new BeginCheckpointLogRecord());
        // Transaction table and dirty table need to flush to disk
        Map<Long, Long> resDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> resTxnTable = new HashMap<>();
        // processing the dirty page table
        for (Long pageNum : dirtyPageTable.keySet()) {
            resDPT.put(pageNum, dirtyPageTable.get(pageNum));
            if (!EndCheckpointLogRecord.fitsInOneRecord(resDPT.size() + 1, resTxnTable.size())) {
                writeEndCheckpointLogRecord(resDPT, resTxnTable);
            }
        }
        // processing the transaction table
        for (Long transNum : transactionTable.keySet()) {
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            resTxnTable.put(transNum,
                    new Pair<>(transactionEntry.transaction.getStatus(), transactionEntry.lastLSN));
            if (!EndCheckpointLogRecord.fitsInOneRecord(resDPT.size(), resTxnTable.size() + 1)) {
                writeEndCheckpointLogRecord(resDPT, resTxnTable);
            }
        }

        // WAL: Writing ahead the ending log record and flush
        long lastLSN = logManager.appendToLog(new EndCheckpointLogRecord(resDPT, resTxnTable));
        flushToLSN(lastLSN);

        // Update master record -> update the begin Checkpoint LSN
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    private void writeEndCheckpointLogRecord(Map<Long, Long> resDPT,
                                             Map<Long, Pair<Transaction.Status, Long>> resTxnTable) {
        logManager.appendToLog(new EndCheckpointLogRecord(resDPT, resTxnTable));
        resDPT.clear();
        resTxnTable.clear();
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        // checkpoint to the logManager that we have already closed it
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        // remove the page not actually dirty in the buffer pool
        this.cleanDPT();
        this.restartUndo();
        // for the next time, we do not need to scan before the this checkpoint
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each table entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record to grab the start checking point
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        Iterator<LogRecord> logs = logManager.scanFrom(LSN);
        // 1. Process the transaction table
        // 2. Process the dirty page table
        // 3. if there is end checking point, update to both table
        while (logs.hasNext()) {
            record = logs.next();
            if (record.getTransNum().isPresent()) {
                processTransaction(record, endedTransactions);
            }
            if (record.getPageNum().isPresent()) {
                processPage(record);
            }
            if (record.getType() == LogType.END_CHECKPOINT) {
                Map<Long, Long> chkptDPT = record.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = record.getTransactionTable();
                processCheckpoint(chkptDPT, chkptTxnTable, endedTransactions);
            }
        }
        // after we have the final transaction and dirty page table
        // we need to changing the status of the transaction table
        for (long transNum : transactionTable.keySet()) {
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            switch (transactionEntry.transaction.getStatus()) {
                case COMMITTING: // if the transaction is committing, marking it is ended
                    transactionEntry.transaction.cleanup();
                    end(transNum);
                    break;
                case RUNNING: // if the transaction still running, terminate it and undo
                    abort(transNum);
                    transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    break;
                default:
                    break;
            }
        }
    }


    /**
     * Called when the log record is for a transaction operation.
     * Updates the transaction table according to the record.
     *
     * @param record log record being processed
     * @param endedTransactions the set of ended transactions
     */
    void processTransaction(LogRecord record, Set<Long> endedTransactions) {
        long transNum = record.getTransNum().get();
        if (!transactionTable.containsKey(transNum)) {
            startTransaction(newTransaction.apply(transNum));
        }
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        Transaction transaction = transactionEntry.transaction;
        transactionEntry.lastLSN = record.getLSN();

        switch (record.getType()) {
            case COMMIT_TRANSACTION:
                transaction.setStatus(Transaction.Status.COMMITTING);
                break;
            case ABORT_TRANSACTION:
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                break;
            case END_TRANSACTION: // really finished, cleanup transaction and remove from table
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
                endedTransactions.add(transNum);
                break;
            default:
                break;
        }
    }

    /**
     * Called when the log record is page-related.
     * Updates the dirty page table according to the record.
     *
     * @param record log record being processed
     */
    void processPage(LogRecord record) {
        long pageNum = record.getPageNum().get();
        switch (record.getType()) {
            case UPDATE_PAGE: case UNDO_UPDATE_PAGE:
                dirtyPageTable.putIfAbsent(pageNum, record.getLSN());
                break;
            case FREE_PAGE: case UNDO_ALLOC_PAGE:
                dirtyPageTable.remove(pageNum);
                break;
            default:
                break;
        }
    }

    /**
     * Called when the log record is an end_checkpoint record.
     * Synchronize the analyzing status with checkpoints.
     *
     * @param chkptDPT DPT in the checkpoint
     * @param chkptTxnTable transaction table in the checkpoint
     */
    void processCheckpoint(Map<Long, Long> chkptDPT,
                           Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable,
                           Set<Long> endedTransactions) {
        // because dirty page contains all the fist updated LSN, we can directly put it inside it
        dirtyPageTable.putAll(chkptDPT);
        // for transaction table, we have several situations:
        // 1. if the transaction is showed up in the endedTransaction: ignore it
        // 2. if the transaction table does not contains current one? => start this transaction
        // 3. compare the two transaction table:
        // 3.1 : the LastLSN should be the largest one (since we want to get the last update LSN)
        // 3.2 : situation that it is aborting in the middle, then we should change it to aborting
        for (long transNum : chkptTxnTable.keySet()) {
            // 1.
            if (endedTransactions.contains(transNum)) continue;
            // 2.
            if (!transactionTable.containsKey(transNum)) {
                startTransaction(newTransaction.apply(transNum));
            }
            // 3.
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            Transaction transaction = transactionEntry.transaction;
            Pair<Transaction.Status, Long> txnInfo = chkptTxnTable.get(transNum);
            // 3.1
            transactionEntry.lastLSN = Math.max(transactionEntry.lastLSN, (long) txnInfo.getSecond());
            // 3.2
            if (transaction.getStatus() == Transaction.Status.RUNNING) {
                Transaction.Status status = txnInfo.getFirst() == Transaction.Status.ABORTING ?
                        Transaction.Status.RECOVERY_ABORTING : txnInfo.getFirst();
                transaction.setStatus(status);
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        if (dirtyPageTable.isEmpty()) {
            return;
        }

        long LSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> logs = logManager.scanFrom(LSN);
        while (logs.hasNext()) {
            LogRecord record = logs.next();
            if (!record.isRedoable()) continue;
            switch (record.type) {
                case ALLOC_PART: case UNDO_ALLOC_PART:
                case FREE_PART: case UNDO_FREE_PART:
                case ALLOC_PAGE: case UNDO_FREE_PAGE:
                    record.redo(this, diskSpaceManager, bufferManager);
                    break;
                case UPDATE_PAGE: case UNDO_UPDATE_PAGE:
                case FREE_PAGE: case UNDO_ALLOC_PAGE:
                    // many cases: try to compare current LSN with flushed LSN and page disk LSN, and rec LSN
                    redoDirtyPage(record);
                    break;
                default:
                    break;
            }
        }
        return;
    }

    /**
     * This helper method redo the record that dirties the page.
     * @param record log record being redone
     */
    void redoDirtyPage(LogRecord record) {
        long pageNum = record.getPageNum().get();
        if (dirtyPageTable.containsKey(pageNum) &&
                record.LSN >= dirtyPageTable.get(pageNum)) {
            Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
            try {
                if (record.LSN > page.getPageLSN()) {
                    record.redo(this, diskSpaceManager, bufferManager);
                }
            } finally {
                page.unpin();
            }
        }
    }

    /**
     * This method performs the undo pass of restart recovery.
     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // Targeting to the transaction that are still on the transaction table
        PriorityQueue<Pair<Long, Long>> queue = new PriorityQueue<>(new PairFirstReverseComparator<Long, Long>());
        for (long transNum : transactionTable.keySet()) {
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            queue.add(new Pair<>(transactionEntry.lastLSN, transNum));
        }

        while (!queue.isEmpty()) {
            Pair<Long, Long> lastTrans = queue.poll();
            long currentLSN = lastTrans.getFirst(), transNum = lastTrans.getSecond();
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            LogRecord current = logManager.fetchLogRecord(currentLSN);
            if (current.isUndoable()) {
                // WAL
                LogRecord CLR = current.undo(transactionEntry.lastLSN);
                transactionEntry.lastLSN = logManager.appendToLog(CLR);
                CLR.redo(this, diskSpaceManager, bufferManager);
            }
            currentLSN = current.getUndoNextLSN().isPresent() ?
                    current.getUndoNextLSN().get() : current.getPrevLSN().get();
            // clean-up if there is nothing to undo for the current transaction
            if (currentLSN == 0) {
                transactionEntry.transaction.cleanup();
                end(transNum);
            } else {
                queue.add(new Pair<>(currentLSN, transNum));
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();

        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
