package io.github.wesleyone.delay;

import java.util.Date;
import java.util.Properties;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageAccessor;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import io.github.wesleyone.MqConfig;

/**
 * 发送延时/定时消息 适配方案
 */
public class ProducerDelayTestAdapter {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // AccessKeyKey、SecretKey授权相关。若启用需要配置,
        // 参考https://wesleyone.github.io/rocketmqCake/best_practice/acl/user_guide.html
        properties.put(PropertyKeyConst.AccessKey, MqConfig.ACCESS_KEY);
        properties.put(PropertyKeyConst.SecretKey, MqConfig.SECRET_KEY);
        // 设置TCP接入域名，即nameserver地址
        properties.put(PropertyKeyConst.NAMESRV_ADDR, MqConfig.NAMESRV_ADDR);

        Producer producer = ONSFactory.createProducer(properties);
        // 在发送消息前，必须调用start方法来启动Producer，只需调用一次即可。
        producer.start();
        long ct = System.currentTimeMillis() + 10 * 1000L;
        // 设置代表消息的业务关键属性，请尽可能全局唯一。
        Message msg = new Message(
                // Message所属的Topic。
                "DelayTopicTestMQ",
                // Message Tag可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在消息队列RocketMQ版的服务器过滤。
                "TagA",
                // Message Body可以是任何二进制形式的数据，消息队列RocketMQ版不做任何干预，需要Producer与Consumer协商好一致的序列化和反序列化方式。
                ("Hello MQ.DELAY."+ct).getBytes());
        // 以方便您在无法正常收到消息情况下，可通过消息队列RocketMQ版控制台查询消息并补发。
        // 注意：不设置也不会影响消息正常收发。
        msg.setKey("ORDERID_100");

        try {
            // 定时消息，单位毫秒（ms），在指定时间戳（当前时间之后）进行投递，例如2016-03-07 16:21:00投递。如果被设置成当前时间戳之前的某个时刻，消息将立即被投递给消费者。
            // msg.setStartDeliverTime(timeStamp);

            // 适配。延时等级1-18等级对应延时"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
            MessageAccessor.putSystemProperties(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL, "3");

            // 发送消息，只要不抛异常就是成功。
            SendResult sendResult = producer.send(msg);
            System.out.println("Message Id:" + sendResult.getMessageId());
        }
        catch (Exception e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
            System.out.println(new Date() + " Send mq message failed. Topic is:" + msg.getTopic());
            e.printStackTrace();
        }

        // 在应用退出前，销毁Producer对象。
        // 注意：如果不销毁也没有问题。
        producer.shutdown();
    }
}
