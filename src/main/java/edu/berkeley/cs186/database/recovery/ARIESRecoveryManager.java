package edu.berkeley.cs186.database.recovery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.Transaction.Status;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.AbortTransactionLogRecord;
import edu.berkeley.cs186.database.recovery.records.AllocPageLogRecord;
import edu.berkeley.cs186.database.recovery.records.AllocPartLogRecord;
import edu.berkeley.cs186.database.recovery.records.BeginCheckpointLogRecord;
import edu.berkeley.cs186.database.recovery.records.CommitTransactionLogRecord;
import edu.berkeley.cs186.database.recovery.records.EndCheckpointLogRecord;
import edu.berkeley.cs186.database.recovery.records.EndTransactionLogRecord;
import edu.berkeley.cs186.database.recovery.records.FreePageLogRecord;
import edu.berkeley.cs186.database.recovery.records.FreePartLogRecord;
import edu.berkeley.cs186.database.recovery.records.MasterLogRecord;
import edu.berkeley.cs186.database.recovery.records.UndoAllocPageLogRecord;
import edu.berkeley.cs186.database.recovery.records.UndoAllocPartLogRecord;
import edu.berkeley.cs186.database.recovery.records.UndoFreePageLogRecord;
import edu.berkeley.cs186.database.recovery.records.UndoFreePartLogRecord;
import edu.berkeley.cs186.database.recovery.records.UndoUpdatePageLogRecord;
import edu.berkeley.cs186.database.recovery.records.UpdatePageLogRecord;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
            Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
            Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter, boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
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
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        transactionTable.get(transNum).transaction.setStatus(Status.COMMITTING);
        long lsn = logManager
                .appendToLog(new CommitTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        transactionTable.get(transNum).lastLSN = lsn;
        logManager.flushToLSN(lsn);
        return lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        transactionTable.get(transNum).transaction.setStatus(Status.ABORTING);
        long lsn = logManager
                .appendToLog(new AbortTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        transactionTable.get(transNum).lastLSN = lsn;
        return lsn;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        if (transactionTable.get(transNum).transaction.getStatus() == Status.ABORTING) {
            rollbackToLSN(transNum, 0);
        }
        transactionTable.get(transNum).transaction.cleanup();
        transactionTable.get(transNum).transaction.setStatus(Status.COMPLETE);
        long lsn = logManager
                .appendToLog(new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        transactionTable.get(transNum).lastLSN = lsn;
        transactionTable.remove(transNum);
        return lsn;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * The general idea is starting the LSN of the most recent record that hasn't
     * been undone:
     * - while the current LSN is greater than the LSN we're rolling back to
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Flush if necessary
     *       - Update the dirty page table if necessary in the following cases:
     *          - You undo an update page record (this is the same as applying
     *            the original update in reverse, which would dirty the page)
     *          - You undo alloc page page record (note that freed pages are no
     *            longer considered dirty)
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        long undoLsn = lastRecordLSN;
        while (currentLSN > LSN) {
            LogRecord record = logManager.fetchLogRecord(currentLSN);
            if (record.isUndoable()) {
                Pair<LogRecord, Boolean> undo = record.undo(undoLsn);
                LogRecord clr = undo.getFirst();
                long clrLsn = logManager.appendToLog(clr);
                transactionEntry.lastLSN = clrLsn;
                undoLsn = clrLsn;
                if (clr instanceof UndoUpdatePageLogRecord || clr instanceof UndoAllocPageLogRecord) {
                    clr.getPageNum().ifPresent(pn -> dirtyPageTable.putIfAbsent(pn, clrLsn));
                }
                if (undo.getSecond()) {
                    logManager.flushToLSN(clrLsn);
                    clr.getPageNum().ifPresent(pn -> dirtyPageTable.remove(pn));
                }
                clr.redo(diskSpaceManager, bufferManager);
            }
            currentLSN = record.getPrevLSN().orElse(LSN);
        }
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
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before, byte[] after) {
        assert (before.length == after.length);

        long lsn;
        if (after.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, transactionTable.get(transNum).lastLSN,
                    pageOffset, before, null));
            lsn = logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum,
                    transactionTable.get(transNum).lastLSN, pageOffset, null, after));
        } else {
            lsn = logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum,
                    transactionTable.get(transNum).lastLSN, pageOffset, before, after));
        }
        transactionTable.get(transNum).lastLSN = lsn;
        transactionTable.get(transNum).touchedPages.add(pageNum);
        dirtyPageTable.putIfAbsent(pageNum, lsn);
        return lsn;
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
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
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
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
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
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
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
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
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

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);
        rollbackToLSN(transNum, LSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            for (long pageNum : entry.getValue().touchedPages) {
                Long lsn = dirtyPageTable.get(pageNum);
                if (lsn != null) {
                    txnTable.put(entry.getKey(), new Pair<>(entry.getValue().transaction.getStatus(), lsn));
                    dpt.put(pageNum, lsn);
                }
            }
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size(),
                            touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size(),
                            touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    @Override
    public void close() {
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
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.cleanDPT();

        return () -> {
            this.restartUndo();
            this.checkpoint();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (free/undoalloc always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     * - The status's in the transaction table should be updated if its possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> committing is a possible transition,
     *   but committing -> running is not.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING. Remove
     * transactions in the COMPLETE state from the transaction table.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        for (Iterator<LogRecord> recordIter = logManager.scanFrom(LSN); recordIter.hasNext();) {
            analyzeLogRecord(recordIter.next());
        }
        endingTransactions();
    }

    private void analyzeLogRecord(LogRecord logRecord) {
        if (logRecord instanceof BeginCheckpointLogRecord) {
            logRecord.getMaxTransactionNum().filter(transNum -> transNum > getTransactionCounter.get())
                    .ifPresent(updateTransactionCounter::accept);
        } else if (logRecord instanceof EndCheckpointLogRecord) {
            analyzeEndCheckpoint(logRecord);
        } else if (logRecord instanceof EndTransactionLogRecord) {
            analyzeTransactionLogRecord(logRecord, entry -> {
                entry.transaction.cleanup();
                entry.transaction.setStatus(Status.COMPLETE);
            });
        } else if (logRecord instanceof CommitTransactionLogRecord) {
            analyzeTransactionLogRecord(logRecord, entry -> entry.transaction.setStatus(Status.COMMITTING));
        } else if (logRecord instanceof AbortTransactionLogRecord) {
            analyzeTransactionLogRecord(logRecord, entry -> entry.transaction.setStatus(Status.RECOVERY_ABORTING));
        } else if (logRecord instanceof UpdatePageLogRecord || logRecord instanceof UndoUpdatePageLogRecord) {
            analyzeTransactionLogRecord(logRecord, entry -> logRecord.getPageNum().ifPresent(pageNum -> {
                addTouchedPage(entry, pageNum);
                dirtyPageTable.put(pageNum, entry.lastLSN);
            }));
        } else if (logRecord instanceof UndoAllocPageLogRecord || logRecord instanceof FreePageLogRecord) {
            analyzeTransactionLogRecord(logRecord, entry -> logRecord.getPageNum().ifPresent(pageNum -> {
                addTouchedPage(entry, pageNum);
                dirtyPageTable.remove(pageNum);
                logManager.flushToLSN(entry.lastLSN);
            }));
        }
    }

    private void analyzeEndCheckpoint(LogRecord logRecord) {
        dirtyPageTable.clear();
        dirtyPageTable.putAll(logRecord.getDirtyPageTable());

        logRecord.getTransactionTable().entrySet().forEach(checkpoint -> {
            TransactionTableEntry entry = getOrNewTransactionTableEntry(checkpoint.getKey());
            if (checkpoint.getValue().getSecond() >= entry.lastLSN) {
                entry.lastLSN = checkpoint.getValue().getSecond();
            }
            if (checkpoint.getValue().getFirst() == Status.COMPLETE) {
                entry.transaction.cleanup();
                entry.transaction.setStatus(Status.COMPLETE);
            } else if (checkpoint.getValue().getFirst() == Status.COMMITTING
                    && entry.transaction.getStatus() != Status.COMPLETE) {
                entry.transaction.setStatus(Status.COMMITTING);
            } else if (checkpoint.getValue().getFirst() == Status.ABORTING
                    && entry.transaction.getStatus() != Status.COMPLETE) {
                entry.transaction.setStatus(Status.RECOVERY_ABORTING);
            }
        });

        logRecord.getTransactionTouchedPages().entrySet()
                .forEach(touchedPages -> Optional.ofNullable(transactionTable.get(touchedPages.getKey()))
                        .filter(entry -> entry.transaction.getStatus() != Status.COMPLETE).ifPresent(
                                entry -> touchedPages.getValue().forEach(pageNum -> addTouchedPage(entry, pageNum))));
    }

    private void analyzeTransactionLogRecord(LogRecord logRecord, Consumer<TransactionTableEntry> consumer) {
        logRecord.getTransNum().ifPresent(transNum -> {
            TransactionTableEntry entry = getOrNewTransactionTableEntry(transNum);
            entry.lastLSN = logRecord.getLSN();
            consumer.accept(entry);
        });
    }

    private void addTouchedPage(TransactionTableEntry entry, Long pageNum) {
        entry.touchedPages.add(pageNum);
        acquireTransactionLock(entry.transaction, getPageLockContext(pageNum), LockType.X);
    }

    private TransactionTableEntry getOrNewTransactionTableEntry(Long transNum) {
        if (!transactionTable.containsKey(transNum)) {
            startTransaction(newTransaction.apply(transNum));
        }
        return transactionTable.get(transNum);
    }

    private void endingTransactions() {
        transactionTable.entrySet().forEach(entry -> {
            if (entry.getValue().transaction.getStatus() == Status.COMPLETE) {
                transactionTable.remove(entry.getKey());
            } else if (entry.getValue().transaction.getStatus() == Status.COMMITTING) {
                entry.getValue().transaction.cleanup();
                entry.getValue().transaction.setStatus(Status.COMPLETE);
                entry.getValue().lastLSN = logManager
                        .appendToLog(new EndTransactionLogRecord(entry.getKey(), entry.getValue().lastLSN));
                transactionTable.remove(entry.getKey());
            } else if (entry.getValue().transaction.getStatus() == Status.RUNNING) {
                entry.getValue().transaction.setStatus(Status.RECOVERY_ABORTING);
                entry.getValue().lastLSN = logManager
                        .appendToLog(new AbortTransactionLogRecord(entry.getKey(), entry.getValue().lastLSN));
            }
        });
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        for (Iterator<LogRecord> recordIter = logManager.scanFrom(Collections.min(dirtyPageTable.values())); recordIter
                .hasNext();) {
            LogRecord logRecord = recordIter.next();
            if (logRecord.isRedoable()) {
                if (logRecord instanceof UpdatePageLogRecord || logRecord instanceof UndoUpdatePageLogRecord
                        || logRecord instanceof AllocPageLogRecord || logRecord instanceof UndoFreePageLogRecord
                        || logRecord instanceof UndoAllocPageLogRecord || logRecord instanceof FreePageLogRecord) {
                    logRecord.getPageNum().ifPresent(pageNum -> Optional.ofNullable(dirtyPageTable.get(pageNum))
                            .filter(recLSN -> logRecord.getLSN() >= recLSN).ifPresent(recLSN -> {
                                Page page = bufferManager.fetchPage(getPageLockContext(pageNum).parentContext(),
                                        pageNum, false);
                                try {
                                    if (page.getPageLSN() < logRecord.getLSN()) {
                                        logRecord.redo(diskSpaceManager, bufferManager);
                                    }
                                } finally {
                                    page.unpin();
                                }
                            }));
                } else if (logRecord instanceof AllocPartLogRecord || logRecord instanceof UndoFreePartLogRecord
                        || logRecord instanceof UndoAllocPartLogRecord || logRecord instanceof FreePartLogRecord) {
                    logRecord.redo(diskSpaceManager, bufferManager);
                }
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        Map<Long, Long> lastLsns = new HashMap<>();
        Queue<Long> lsnQueue = new PriorityQueue<>(Comparator.reverseOrder());
        transactionTable.entrySet().stream()
                .filter(entry -> entry.getValue().transaction.getStatus() == Status.RECOVERY_ABORTING)
                .forEach(entry -> {
                    lsnQueue.add(entry.getValue().lastLSN);
                    lastLsns.put(entry.getKey(), entry.getValue().lastLSN);
                });
        for (Long lsn = lsnQueue.poll(); lsn != null; lsn = lsnQueue.poll()) {
            LogRecord logRecord = logManager.fetchLogRecord(lsn);
            if (logRecord.isUndoable()) {
                logRecord.getTransNum().ifPresent(transNum -> {
                    Pair<LogRecord, Boolean> undo = logRecord.undo(lastLsns.get(transNum));
                    lastLsns.put(transNum, logManager.appendToLog(undo.getFirst()));
                    if (undo.getSecond()) {
                        logManager.flushToLSN(undo.getFirst().LSN);
                    }
                    if (undo.getFirst() instanceof UndoUpdatePageLogRecord) {
                        undo.getFirst().getPageNum()
                                .ifPresent(pageNum -> dirtyPageTable.put(pageNum, undo.getFirst().LSN));
                    }
                    undo.getFirst().LSN = transactionTable.get(transNum).lastLSN;
                    undo.getFirst().redo(diskSpaceManager, bufferManager);
                });
            }
            long nextLsn = logRecord.getUndoNextLSN().or(logRecord::getPrevLSN).orElse(0L);
            if (nextLsn > 0L) {
                lsnQueue.add(nextLsn);
            } else {
                logRecord.getTransNum().map(transactionTable::get).map(entry -> entry.transaction)
                        .ifPresent(transaction -> {
                            transaction.cleanup();
                            transaction.setStatus(Status.COMPLETE);
                            logManager.appendToLog(new EndTransactionLogRecord(transaction.getTransNum(),
                                    lastLsns.get(transaction.getTransNum())));
                            transactionTable.remove(transaction.getTransNum());
                        });
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager. THIS IS SLOW
     * and should only be used during recovery.
     */
    private void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty)
                dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext, LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext, LockContext lockContext,
            LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "("
                        + lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }
}
