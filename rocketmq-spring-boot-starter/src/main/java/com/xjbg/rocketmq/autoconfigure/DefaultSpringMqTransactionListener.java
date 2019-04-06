package com.xjbg.rocketmq.autoconfigure;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.HashSet;
import java.util.Set;

/**
 * must be thread safety
 *
 * @author kesc
 * @since 2019/4/3
 */
public class DefaultSpringMqTransactionListener implements TransactionListener {
    private Set<String> transactionSet = new HashSet<>();

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                transactionSet.add(msg.getTransactionId());
            }
        });
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        boolean contains = transactionSet.contains(msg.getTransactionId());
        if (contains) {
            transactionSet.remove(msg.getTransactionId());
            return LocalTransactionState.COMMIT_MESSAGE;
        }
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
