package com.xjbg.rocketmq.enums;

import lombok.Getter;

/**
 * @author kesc
 * @since 2019/4/4
 */
@Getter
public enum MessageListenerType {
    CONCURRENTLY("concurrently"),
    ORDERLY("orderly"),
    BATCH("batch"),
    DEFAULT("default");
    private String typeCN;

    MessageListenerType(String typeCN) {
        this.typeCN = typeCN;
    }
}
