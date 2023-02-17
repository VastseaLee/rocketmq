package com.young.springbootmq.config;

import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;

@RocketMQTransactionListener(rocketMQTemplateBeanName = "rocketMQTemplate")
public class MyTransaction implements RocketMQLocalTransactionListener {

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object o) {
        Integer integer = (Integer) o;

        if(integer == 1){
            //发出的消息可见，会被消费
            return RocketMQLocalTransactionState.COMMIT;
        }else if(integer == 2){
            //发出的消息会被丢弃，也就是无人能消费
            return RocketMQLocalTransactionState.ROLLBACK;
        }else {
            //会不断回调下面那个方法
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        return null;
    }
}
