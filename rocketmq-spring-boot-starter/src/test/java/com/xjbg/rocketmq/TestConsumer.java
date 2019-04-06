package com.xjbg.rocketmq;

import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.xjbg.rocketmq.autoconfigure.Consumer;
import com.xjbg.rocketmq.enums.ConsumerType;
import com.xjbg.rocketmq.message.MessageBuilder;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author kesc
 * @since 2019/4/6
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class TestConsumer {
    private static final String testTopic = "message-center-topic";
    @Autowired(required = false)
    private DefaultMQProducer mqProducer;
    @Autowired(required = false)
    private ProducerBean producerBean;

    @Consumer(consumerGroup = "test-group", topic = testTopic, consumerType = ConsumerType.PUSH)
    public void test(String a) {
        System.out.println(a);
    }

    @Test
    public void run() throws Exception {
        if (mqProducer != null) {
            mqProducer.send(MessageBuilder.newProfileBuilder().topic(testTopic).body(1).getRocketMQMessage());
        }
        if (producerBean != null) {
            producerBean.send(MessageBuilder.newProfileBuilder().topic(testTopic).body(1).getOnsMessage());
        }
        Thread.sleep(30_000);
    }
}
