package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType currentLockType = lockContext.lockman.getLockType(transaction, lockContext.name);

        // TODO(proj4_part2): implement
        if (LockType.substitutable(effectiveLockType, requestType)) {
            // The current lock type can effectively substitute the requested type
            return;
        } else if (currentLockType == LockType.IX && requestType == LockType.S) {
            // The current lock type is IX and the requested lock is S
            requestType = LockType.SIX;
            ensureSufficientIntent(parentContext, requestType);
            lockContext.promote(transaction, requestType);
        } else if (currentLockType.isIntent()) {
            // The current lock type is an intent lock
            ensureSufficientIntent(parentContext, requestType);
            lockContext.escalate(transaction);
            return;
        } else {
            ensureSufficientIntent(parentContext, requestType);
            if (currentLockType == LockType.NL) {
                lockContext.acquire(transaction, requestType);
            } else {
                lockContext.promote(transaction, requestType);
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    public static void ensureSufficientIntent(LockContext lockContext, LockType requestType) {
        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        LockContext parentContext = lockContext.parentContext();
        LockType currentLockType = lockContext.lockman.getLockType(transaction, lockContext.name);

        LockType requestIntent = LockType.parentLock(requestType);
        if (!LockType.canBeParentLock(currentLockType, requestType)) {
            if (currentLockType == LockType.S && requestIntent == LockType.IX) {
                requestIntent = LockType.SIX;
            }

            ensureSufficientIntent(parentContext, requestIntent);
            if (currentLockType == LockType.NL) {
                lockContext.acquire(transaction, requestIntent);
            } else {
                lockContext.promote(transaction, requestIntent);
            }
        }
    }
}
