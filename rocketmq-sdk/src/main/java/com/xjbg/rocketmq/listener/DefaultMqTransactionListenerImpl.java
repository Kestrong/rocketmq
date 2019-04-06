package com.xjbg.rocketmq.listener;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.HashSet;
import java.util.Set;

/**
 * must be thread safety
 *
 * @author kesc
 * @since 2019/4/3
 */
public class DefaultMqTransactionListenerImpl implements TransactionListener {
    private Set<String> transactionSet = new HashSet<>();

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        transactionSet.add(msg.getTransactionId());
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
