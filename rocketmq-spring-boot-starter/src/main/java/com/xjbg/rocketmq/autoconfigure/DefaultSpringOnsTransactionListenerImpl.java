package com.xjbg.rocketmq.autoconfigure;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;
import com.xjbg.rocketmq.listener.DefaultOnsTransactionListenerImpl;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.HashSet;
import java.util.Set;

/**
 * must be thread safety
 *
 * @author kesc
 * @since 2019/4/4
 */
public class DefaultSpringOnsTransactionListenerImpl extends DefaultOnsTransactionListenerImpl {
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
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                transactionSet.add(msg.getMsgID());
            }
        });
        return TransactionStatus.CommitTransaction;
    }
}
