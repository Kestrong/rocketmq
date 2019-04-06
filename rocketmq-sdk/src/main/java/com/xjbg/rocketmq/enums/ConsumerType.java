package com.xjbg.rocketmq.enums;

import lombok.Getter;

/**
 * @author kesc
 * @since 2019/4/4
 */
@Getter
public enum ConsumerType {
    BATCH("batch"),
    ORDERLY("orderly"),
    DEFAULT("default"),
    PULL("pull"),
    PUSH("push"),;
    private String typeCN;

    ConsumerType(String typeCN) {
        this.typeCN = typeCN;
    }
}
