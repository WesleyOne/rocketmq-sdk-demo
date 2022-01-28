package io.github.wesleyone.trace;

import java.util.List;

import io.github.wesleyone.MqConfig;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * 社区版-消费者链路
 */
public class RocketMQPushConsumer {
    /**
     * 替换为您阿里云账号的AccessKey ID和AccessKey Secret。
     */
    private static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(MqConfig.ACCESS_KEY, MqConfig.SECRET_KEY));
    }
    public static void main(String[] args) throws MQClientException {
        /**
         * 创建Consumer，并开启消息轨迹。设置为您在阿里云消息队列RocketMQ版控制台创建的Group ID。
         * 如果不想开启消息轨迹，可以按照如下方式创建：
         * DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("YOUR GROUP ID", getAclRPCHook(), new AllocateMessageQueueAveragely());
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TRACE_C_GROUP_ID", getAclRPCHook(), new AllocateMessageQueueAveragely(), true, null);
        //设置为阿里云消息队列RocketMQ版实例的接入点。
        consumer.setNamesrvAddr(MqConfig.NAMESRV_ADDR);
        //阿里云上消息轨迹需要设置为CLOUD方式，在使用云上消息轨迹的时候，需要设置此项，如果不开启消息轨迹功能，则运行不设置此项。
        consumer.setAccessChannel(AccessChannel.LOCAL);
        // 设置为您在阿里云消息队列RocketMQ版控制台上创建的Topic。
        consumer.subscribe("TRACE_TOPIC", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                System.out.printf("Receive New Messages: %s %n", msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
