package com.young.consumer.receiver;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ConsumerTest {

    public static void main(String[] args) {

//        randomConsume();
        for (int i = 0; i < 3; i++) {
            new Thread(()->{
                try {
//                    orderConsume();
                    broadcastingConsume("consumer"+Math.random() * 100000);
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            }).start();
        }

    }

    private static void broadcastingConsume(String instanceName)throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_1");
        consumer.setMessageModel(MessageModel.BROADCASTING);
//        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setInstanceName(instanceName);
        // Specify name server addresses.
        consumer.setNamesrvAddr("192.168.2.112:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("YoungTopic", "*");
//        consumer.subscribe("YoungTopic", MessageSelector.byTag("TagA"));
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                msgs.forEach(messageExt -> {
                    try {
                        System.out.println(Thread.currentThread().getName()+":"+new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }

        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf(instanceName + " Started.%n");
    }


    private static void orderConsume() throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_1");

        // Specify name server addresses.
        consumer.setNamesrvAddr("192.168.2.112:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("YoungTopic", "*");
//        consumer.subscribe("YoungTopic", MessageSelector.byTag("TagA"));
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext consumeOrderlyContext) {
                msgs.forEach(messageExt -> {
                    try {
                        System.out.println(Thread.currentThread().getName() + ":" + new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");

    }

    /**
     * 乱序消费
     *
     * @throws MQClientException
     */
    private static void randomConsume() throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_1");

        // Specify name server addresses.
        consumer.setNamesrvAddr("192.168.2.112:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("YoungTopic", "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                msgs.forEach(messageExt -> {
                    try {
                        System.out.println(new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
