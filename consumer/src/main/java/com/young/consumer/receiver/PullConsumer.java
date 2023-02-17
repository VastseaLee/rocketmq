package com.young.consumer.receiver;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;

public class PullConsumer {

    public static void main(String[] args) throws MQClientException {
        consume();
    }

    /**
     * 乱序消费
     *
     * @throws MQClientException
     */
    private static void consume() throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("group_1");

        // Specify name server addresses.
        consumer.setNamesrvAddr("192.168.2.166:9876");
        consumer.start();
        // Subscribe one more more topics to consume.
        consumer.subscribe("YoungTopic2", "*");
        // Register callback to execute on arrival of messages fetched from brokers.

        List<MessageExt> msgs = consumer.poll();
        while (msgs.size() != 0){
            msgs.forEach(messageExt -> {
                try {
                    System.out.println(new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            });
            msgs = consumer.poll();
        }

        System.out.println("结束了");

    }
}
