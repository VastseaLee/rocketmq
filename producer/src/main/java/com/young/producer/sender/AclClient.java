package com.young.producer.sender;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;


public class AclClient {

    private static final String ACL_ACCESS_KEY = "RocketMQ";
    private static final String ACL_SECRET_KEY = "12345678";

    public static void main(String[] args) throws Exception {
        producer();
    }

    //在broker端开启aclEnable=true配置,然后配置账户以及账户的权限
    private static void producer() throws Exception {

//        DefaultMQProducer producer = new DefaultMQProducer("group_name", AclUtils.getAclRPCHook(""));
        DefaultMQProducer producer = new DefaultMQProducer("group_name", getAclRPCHook());

        producer.setNamesrvAddr("192.168.2.166:9876");

        producer.start();

        for (int i = 0; i < 10; i++) {
            Message msg = new Message("AclTopic", ("Hello RocketMQ1 " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult1 = producer.send(msg);
            System.out.printf("%s%n", sendResult1);
        }
    }

    private static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(ACL_ACCESS_KEY,ACL_SECRET_KEY));
    }
}
