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
     * - None of the above
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) {
            return;
        }

        // You may find these variables useful
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        if (requestType == explicitLockType) {
            return;
        }
        if (effectiveLockType == LockType.X) {
            return;
        }
        if (requestType == LockType.S && effectiveLockType == LockType.S) {
            return;
        }

        lock(lockContext, transaction, requestType);
    }

    private static void lock(LockContext lockContext, TransactionContext transaction, LockType requestType) {
        LockType lockType = lockContext.getExplicitLockType(transaction);
        if (lockType == LockType.NL) {
            lockAncestors(lockContext.parentContext(), transaction, LockType.parentLock(requestType));
            lockContext.acquire(transaction, requestType);
        } else if (LockType.substitutable(requestType, lockType)) {
            lockAncestors(lockContext.parentContext(), transaction, LockType.parentLock(requestType));
            lockContext.promote(transaction, requestType);
        } else if (requestType == LockType.S && lockType == LockType.IX) {
            lockContext.promote(transaction, LockType.SIX);
        } else if (lockType.isIntent()) {
            lockContext.escalate(transaction);
        }
    }

    private static void lockAncestors(LockContext lockContext, TransactionContext transaction, LockType requestType) {
        if (lockContext == null) {
            return;
        }
        lockAncestors(lockContext.parentContext(), transaction, requestType);

        LockType lockType = lockContext.getExplicitLockType(transaction);
        if (requestType == lockType) {
            return;
        }
        if (lockType == LockType.NL) {
            lockContext.acquire(transaction, requestType);
        } else if (LockType.substitutable(requestType, lockType)) {
            lockContext.promote(transaction, requestType);
        }
    }
}
