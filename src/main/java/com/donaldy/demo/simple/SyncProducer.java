package com.donaldy.demo.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class SyncProducer {
    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new
                DefaultMQProducer("producer-group");

        producer.setNamesrvAddr("localhost:9876");

        producer.start();
        for (int i = 0; i < 100; i++) {

            Message msg = new Message("TopicTest" , // 主题
                    "TagA", // 标签
                    ("Hello RocketMQ " + i)  // key, 用户自定义的key, 唯一的标识
                            .getBytes(RemotingHelper.DEFAULT_CHARSET)
            );

            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }

        producer.shutdown();
    }
}