package io.github.wesleyone.trace;

import java.util.Date;

import io.github.wesleyone.MqConfig;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * 社区版-发送者链路
 */
public class RocketMQProducer {
    /**
     * 替换为您阿里云账号的AccessKey ID和AccessKey Secret。
     */
    private static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(MqConfig.ACCESS_KEY, MqConfig.SECRET_KEY));
    }

    public static void main(String[] args) throws MQClientException {
        /**
         *创建Producer，并开启消息轨迹。设置为您在阿里云消息队列RocketMQ版控制台创建的Group ID。
         *如果不想开启消息轨迹，可以按照如下方式创建：
         *DefaultMQProducer producer = new DefaultMQProducer("YOUR GROUP ID", getAclRPCHook());
         */
        DefaultMQProducer producer = new DefaultMQProducer("TRACE_GROUP_ID", getAclRPCHook(), true, null);
        /**
         *设置使用接入方式为阿里云，在使用云上消息轨迹的时候，需要设置此项，如果不开启消息轨迹功能，则运行不设置此项。
         */
        producer.setAccessChannel(AccessChannel.LOCAL);
        /**
         *设置为您从阿里云消息队列RocketMQ版控制台获取的接入点信息，类似“http://MQ_INST_XXXX.aliyuncs.com:80”。
         */
        producer.setNamesrvAddr(MqConfig.NAMESRV_ADDR);
        producer.start();

        for (int i = 0; i < 1; i++) {
            try {
                Message msg = new Message("TRACE_TOPIC",
                        "TAGA",
                        "Hello world.TRACE".getBytes());
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                //消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                System.out.println(new Date() + " Send mq message failed.");
                e.printStackTrace();
            }
        }

        //在应用退出前，销毁Producer对象。
        //注意：如果不销毁也没有问题。
        producer.shutdown();
    }
}
