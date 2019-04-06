package com.xjbg.rocketmq.listener;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionExecuter;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;

import java.util.HashSet;
import java.util.Set;

/**
 * must be thread safety
 *
 * @author kesc
 * @since 2019/4/4
 */
public class DefaultOnsTransactionListenerImpl implements LocalTransactionExecuter, LocalTransactionChecker {
    private Set<String> transactionSet = new HashSet<>();

    @Override
    public TransactionStatus check(Message msg) {
        boolean contains = transactionSet.contains(msg.getMsgID());
        if (contains) {
            transactionSet.remove(msg.getMsgID());
            return TransactionStatus.CommitTransaction;
        }
        return TransactionStatus.RollbackTransaction;
    }

    @Override
    public TransactionStatus execute(Message msg, Object arg) {
        transactionSet.add(msg.getMsgID());
        return TransactionStatus.CommitTransaction;
    }
}
