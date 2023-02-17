package com.young.producer.sender;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerTest {

    public static void main(String[] args) throws Exception {
        synSend();
    }



    /**
     * 同步发送
     *
     * @throws Exception
     */
    public static void synSend() throws Exception {
        DefaultMQProducer producer = new
                DefaultMQProducer("group_1");

        producer.setNamesrvAddr("192.168.2.166:9876");
        producer.start();
        String[] tags = new String[]{"TagA","TagB","TagC"};
        for (int i = 0; i < 10; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg1 = new Message("YoungTopic",tags[i % 3], ("Hello RocketMQ1 " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            //自定义一些配置
//            msg1.putUserProperty("key","value");


            //延迟级别 有固定的配置  阿里云商业版可以直接指定时间
//            msg1.setDelayTimeLevel(1);

            //Selector可以不传，传了可以指定队列，主要是为了顺序消费
            SendResult sendResult1 = producer.send(msg1, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    return list.get((Integer)o);
                }
            },2);
//            SendResult sendResult2 = producer.send(msg2);
            System.out.printf("%s%n", sendResult1);
//            System.out.printf("%s%n", sendResult2);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }

    /**
     * 异步发送
     *
     * @throws Exception
     */
    public static void asynSend() throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // Specify name server addresses.
        producer.setNamesrvAddr("192.168.2.112:9876");
        //Launch the instance.
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = 100;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                Message msg = new Message("Jodie_topic_1023",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }

    /**
     * 单向发送
     *
     * @throws Exception
     */
    public static void oneWaySend() throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // Specify name server addresses.
        producer.setNamesrvAddr("192.168.2.112:9876");
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 100; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            producer.sendOneway(msg);
        }
        //Wait for sending to complete
        Thread.sleep(5000);
        producer.shutdown();
    }

    /**
     * 批量发送
     *
     * @throws Exception
     */
    public static void batchSend() throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // Specify name server addresses.
        producer.setNamesrvAddr("192.168.2.166:9876");
        //Launch the instance.
        producer.start();
        List<Message> messageList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello Batch " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            messageList.add(msg);
            //Call send message to deliver message to one of brokers.
//            producer.sendOneway(msg);
        }
        SendResult send = producer.send(messageList);
        System.out.println(send);
        //Wait for sending to complete
        producer.shutdown();
    }


    public static void transactionSend() throws MQClientException {
        TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("192.168.2.166:9876");

        producer.start();
    }
}
