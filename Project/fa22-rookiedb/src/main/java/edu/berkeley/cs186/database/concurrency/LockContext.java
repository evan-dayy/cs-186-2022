package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multi-granularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    // a resource may have different locks from different transactions
    // use this to specific how many children context exactly
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        // n1 must be database, since every resource name start from here
        String n1 = names.next();
        // directly create a database context
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }


    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        if (readonly) throw new UnsupportedOperationException();
        if (!multigranularityCheck(transaction, lockType)) {
            // if the lock type is share, then the parent lock should be IS or IX
            throw new InvalidLockException("the request is invalid");
        }
        lockman.acquire(transaction, name, lockType);
        if (parent != null) {
            parent.numChildLocks.put(transaction.getTransNum(),
                    parent.getNumChildren(transaction) + 1);
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        if (readonly) throw new UnsupportedOperationException();
        if (getNumChildren(transaction) != 0) {
            // cannot release the lock since there are children down there
            throw new InvalidLockException("the lock cannot be released");
        }
        lockman.release(transaction, name);
        if (parent != null) {
            parent.numChildLocks.put(transaction.getTransNum(),
                    parent.getNumChildren(transaction) - 1);
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) throw new UnsupportedOperationException();

        LockType lockType = lockman.getLockType(transaction, name);
        if ((!LockType.substitutable(newLockType, lockType) || newLockType == lockType) &&
                !(newLockType == LockType.SIX && (lockType == LockType.IS || lockType == LockType.IX))) {
            throw new InvalidLockException("the requested lock type is not a promotion");
        } else if (!multigranularityCheck(transaction, newLockType)) {
            throw new InvalidLockException("the request is invalid");
        } else if (hasSIXAncestor(transaction) && newLockType == LockType.SIX) {
            throw new InvalidLockException("cannot promote to SIX with a SIX ancestor");
        }

        if (newLockType == LockType.SIX) {
            List<ResourceName> releaseNames = sisDescendants(transaction);
            if (lockType != LockType.NL) {
                releaseNames.add(name);
            }
            lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);
            clearNumChildLocks(transaction, releaseNames, name);
        } else {
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     * @aims get a coarse-grained lock at a higher level and release its lock on its children
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (readonly) throw new UnsupportedOperationException();
        LockType newLockType = escalateType(transaction);
        List<ResourceName> releaseNames = descendantWithLocks(transaction);
        releaseNames.add(name);
        if (lockman.getLockType(transaction, name) != newLockType) {
            lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);
            clearNumChildLocks(transaction, releaseNames, name);
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockType lockType = lockman.getLockType(transaction, name);
        return lockType.getExplicit();
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockType lockType = getExplicitLockType(transaction);
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (name.isDescendantOf(lock.name) &&
                    LockType.substitutable(lock.lockType.getExplicit(), lockType)) {
                lockType = lock.lockType.getExplicit();
            }
        }
        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (name.isDescendantOf(lock.name) && lock.lockType == LockType.SIX) {
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> names = new ArrayList<>();
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name) &&
                    (lock.lockType == LockType.S || lock.lockType == LockType.IS)) {
                names.add(lock.name);
            }
        }
        return names;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }


    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    /**
     * Helper method to see if the new lock type satisfies the multigranularity
     * constraints for the given transaction.
     * @param transaction the given transaction
     * @param newLockType the new lock type to be assigned
     * @return true if multigranularity check passes, false if not
     */
    private boolean multigranularityCheck(TransactionContext transaction, LockType newLockType) {
        if (parent != null) {
            LockType parentLockType = lockman.getLockType(transaction, parent.getResourceName());
            return LockType.canBeParentLock(parentLockType, newLockType);
        } else {
            return true;
        }
    }

    /**
     * Helper method to determine the least permissive lock type for escalation
     * for the given transaction.
     * @param transaction the given transaction
     * @return the least permissive lock type for escalation
     */
    private LockType escalateType(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if ((lock.name.isDescendantOf(name) || lock.name == name) &&
                    (lock.lockType != LockType.IS && lock.lockType != LockType.S)) {
                return LockType.X;
            }
        }
        return LockType.S;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are
     * descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants
     */
    private List<ResourceName> descendantWithLocks(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> names = new ArrayList<>();
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name)) {
                names.add(lock.name);
            }
        }
        return names;
    }

    /**
     * Helper method to clear numChildLocks in the contexts corresponding to
     * a list of resourceNames for the given transaction.
     * @param transaction the given transaction
     * @param names a list of resourceNames to release
     */
    private void clearNumChildLocks(TransactionContext transaction, List<ResourceName> names, ResourceName except) {
        for (ResourceName name : names) {
            if (name != except && name.parent() != null) {
                LockContext context = fromResourceName(lockman, name.parent());
                context.numChildLocks.put(transaction.getTransNum(), context.getNumChildren(transaction) - 1);
            }
        }
    }
}

