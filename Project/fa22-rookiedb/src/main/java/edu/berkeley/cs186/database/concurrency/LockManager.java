package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // dimension 1. How many locks in one transaction
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // dimension 2. How may locks in one resource (such as one table, one record)
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         *
         * @except since one transaction can only one lock at one time, as a result
         * if the lock is the transaction itself, we do not need to care on it.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            for (Lock lock : locks) {
                if (lock.transactionNum == except) continue;
                if (!LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`.
         * Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        public void grantOrUpdateLock(Lock newLock) {
            for (Lock lock : locks) {
                if (lock.transactionNum == newLock.transactionNum) {
                    // as we mentioned in previous @except, the the lock's transaction
                    // is same as the newLock's transaction, directly change lock type
                    lock.lockType = newLock.lockType;
                    return;
                }
            }
            // update two dimension:
            // transaction's lock
            // resource's lock
            locks.add(newLock);
            transactionLocks.computeIfAbsent(newLock.transactionNum, (x) -> new ArrayList<>()).add(newLock);
        }


        /**
         * Acquire the lock in the request `request` and add the request to the
         * queue if not possible. All locks currectly held by the transaction
         * will be updated by the lock in the request.
         */
        public boolean acquireLock(LockRequest request, boolean addFront) {
            if (checkCompatible(request.lock.lockType, request.lock.transactionNum) &&
                    (addFront || waitingQueue.isEmpty())) {
                // two purpose:
                // 1. add or update the lock
                // 2. release all the locks that in the request released
                processRequest(request);
                return false;
            } else {
                addToQueue(request, addFront);
                // tell outside the transaction should be blocked
                request.transaction.prepareBlock();
                return true;
            }
        }

        /**
         * Process the request by updating the lock in the current resource and
         * releasing locks in other resources.
         */
        private void processRequest(LockRequest request) {
            grantOrUpdateLock(request.lock);
            for (Lock lock : request.releasedLocks) {
                ResourceEntry resourceEntry = getResourceEntry(lock.name);
                if (resourceEntry != this) {
                    resourceEntry.releaseLock(lock);
                }
            }
        }

        /**
         * Releases the lock `lock` and processes the queue.
         * Assumes that the lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // current resource remove it
            locks.remove(lock);
            // corresponding transaction remove it
            transactionLocks.get(lock.transactionNum).remove(lock);
            // only grab the first-X continuous locks
            // there are no optimization here
            // a possible optimization is that do as MongoDB do, grab
            // all compatible locks in the queue.
            processQueue();
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                if (checkCompatible(request.lock.lockType, request.lock.transactionNum)) {
                    processRequest(request);
                    waitingQueue.removeFirst();
                    // unblock the current transaction
                    // this will invoke the signal method in Condition class
                    // and another thread to change the blocked status
                    request.transaction.unblock();
                } else {
                    break;
                }
            }
        }


        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        boolean shouldBlock = false;
        // Synchronized block, to make sure the call to this block are serial are there
        // should not have interleaving of different call
        synchronized (this) {
            // the name has one lock of this transaction and it did not plan
            // to release it, then suggesting the transaction add two locks to this resource
            if (!releaseNames.contains(name)) {
                checkDuplicateLockRequest(transaction, name);
            }
            // check whether the transaction have that lock in the released resource
            for (ResourceName releaseName : releaseNames) {
                checkNoLockHeld(transaction, releaseName);
            }
            List<Lock> releasedLocks = new ArrayList<>();
            for (ResourceName releaseName : releaseNames) {
                LockType releaseLockType = getLockType(transaction, releaseName);
                Lock lock = new Lock(releaseName, releaseLockType, transaction.getTransNum());
                releasedLocks.add(lock);
            }

            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, lock, releasedLocks);
            shouldBlock = getResourceEntry(name).acquireLock(request, true);
        }
        // why we should block outside the synchronized block?
        // synchronized block only allow one thread to access and
        // if you block inside it, you put the entire thread in a infinite loop
        // only lock Manager can unblock, but right now if we are in synchronized block,
        // the logManager cannot accept any other call
        if (shouldBlock) {
            // put the transaction's thread into a while infinite loop
            // need another thread to change the status
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        boolean shouldBlock = false;
        synchronized (this) {
            checkDuplicateLockRequest(transaction, name);
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, lock);
            shouldBlock = getResourceEntry(name).acquireLock(request, false);
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        synchronized (this) {
            checkNoLockHeld(transaction, name);
            LockType lockType = getLockType(transaction, name);
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            getResourceEntry(name).releaseLock(lock);
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        boolean shouldBlock = false;
        synchronized (this) {
            checkDuplicateLockRequest(transaction, name, newLockType);
            checkNoLockHeld(transaction, name);
            checkInvalidLock(transaction, name, newLockType);
            Lock lock = new Lock(name, newLockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, lock);
            shouldBlock = getResourceEntry(name).acquireLock(request, true);
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    //////////////// Helper Method and Customer Check
    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    private synchronized void checkDuplicateLockRequest(TransactionContext transaction,
                                                        ResourceName name)
            throws DuplicateLockRequestException {
        LockType lockType = getLockType(transaction, name);
        if (lockType != LockType.NL) {
            throw new DuplicateLockRequestException(
                    "a lock on `" + name.toString() + "` is held by `" + transaction.toString() + "`"
            );
        }
    }

    private synchronized void checkDuplicateLockRequest(TransactionContext transaction,
                                                        ResourceName name, LockType newLockType)
            throws DuplicateLockRequestException {
        LockType lockType = getLockType(transaction, name);
        if (lockType == newLockType) {
            throw new DuplicateLockRequestException(
                    "a lock on `" + name.toString() + "` is held by `" + transaction.toString() + "`"
            );
        }
    }

    private synchronized void checkNoLockHeld(TransactionContext transaction, ResourceName name)
            throws DuplicateLockRequestException {
        LockType lockType = getLockType(transaction, name);
        if (lockType == LockType.NL) {
            throw new NoLockHeldException(
                    "no lock on `" + name.toString() + "` is held by `" + transaction.toString() + "`"
            );
        }
    }

    /**
     *  the method is within a synchronized block
     */
    private synchronized void checkInvalidLock(TransactionContext transaction, ResourceName name,
                                               LockType newLockType)
            throws InvalidLockException {
        LockType lockType = getLockType(transaction, name);
        if (LockType.substitutable(lockType, newLockType) || lockType == newLockType) {
            throw new InvalidLockException(
                    "the requested lock type is not a promotion"
            );
        }
    }

    /////////// Context Related

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }

}
