package com.young.consumer.receiver;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ConsumerTest {

    public static void main(String[] args) throws MQClientException {

        randomConsume();

    }

    /**
     * 广播消费
     *
     * @param instanceName
     * @throws MQClientException
     */
    private static void broadcastingConsume(String instanceName) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_1");

        //默认是集群消费
        consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.setInstanceName(instanceName);
        consumer.setNamesrvAddr("192.168.2.112:9876");

        // Subscribe one more more topics to consume.
//        consumer.subscribe("YoungTopic", "*");
//        consumer.subscribe("YoungTopic", MessageSelector.byTag("TagA"));
//        consumer.subscribe("YoungTopic", MessageSelector.byTag("TagA || TagB"));
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                msgs.forEach(messageExt -> {
                    try {
                        System.out.println(Thread.currentThread().getName() + ":" + new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
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


    /**
     * 顺序消费
     *
     * @throws MQClientException
     */
    private static void orderConsume() throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_1");

        // Specify name server addresses.
        consumer.setNamesrvAddr("192.168.2.166:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("YoungTopic", "*");
//        consumer.subscribe("YoungTopic2", "*");
//        consumer.subscribe("YoungTopic", MessageSelector.byTag("TagA"));
        // Register callback to execute on arrival of messages fetched from brokers.

        //顺序消费在客户端主要是选择不一样的监听器,只能保证一个队列内的消息有序,所以在生产者端需要指定特定的队列
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
        consumer.setNamesrvAddr("192.168.2.166:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("TopicTest", "*");
        // 过滤是在broker端直接进行的，可以减少网络IO
//        consumer.subscribe("YoungTopic", MessageSelector.byTag("TagA || TagB"));
        //sql过滤默认是不支持的，需要修改配置文件
//        consumer.subscribe("YoungTopic", MessageSelector.bySql("TAGS is not null and TAGS IN ('TagA','TagB')"));
//        consumer.subscribe("YoungTopic2", "*");
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
