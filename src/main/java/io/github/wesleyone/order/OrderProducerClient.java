package io.github.wesleyone.order;

import java.util.Date;
import java.util.Properties;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.order.OrderProducer;
import io.github.wesleyone.MqConfig;

/**
 * 发送顺序消息
 * - 分区顺序。按照Sharding Key进行区块分区选择。
 * - 全局顺序。创建topic时只设置1个队列。
 *
 * 全局顺序消息和分区顺序消息的发布方式基本一样，示例代码如下。
 */
public class OrderProducerClient {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // AccessKeyKey、SecretKey授权相关。若启用需要配置,
        // 参考https://wesleyone.github.io/rocketmqCake/best_practice/acl/user_guide.html
        properties.put(PropertyKeyConst.AccessKey, MqConfig.ACCESS_KEY);
        properties.put(PropertyKeyConst.SecretKey, MqConfig.SECRET_KEY);
        // 设置TCP接入域名，即nameserver地址
        properties.put(PropertyKeyConst.NAMESRV_ADDR, MqConfig.NAMESRV_ADDR);

        OrderProducer producer = ONSFactory.createOrderProducer(properties);
        // 在发送消息前，必须调用start方法来启动Producer，只需调用一次即可。
        producer.start();
        for (int i = 0; i < 10; i++) {
            String orderId = "biz_" + i % 10;
            Message msg = new Message(
                    // Message所属的Topic。
                    "Order_global_topic",
                    // Message Tag，可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在消息队列RocketMQ版的服务器过滤。
                    "TagA",
                    // Message Body，可以是任何二进制形式的数据，消息队列RocketMQ版不做任何干预，需要Producer与Consumer协商好一致的序列化和反序列化方式。
                    "send order global msg".getBytes()
            );
            // 设置代表消息的业务关键属性，请尽可能全局唯一。
            // 以方便您在无法正常收到消息情况下，可通过消息队列RocketMQ版控制台查询消息并补发。
            // 注意：不设置也不会影响消息正常收发。
            msg.setKey(orderId);
            // 分区顺序消息中区分不同分区的关键字段，Sharding Key与普通消息的key是完全不同的概念。
            // 全局顺序消息，该字段可以设置为任意非空字符串。
            String shardingKey = orderId;
            try {
                SendResult sendResult = producer.send(msg, shardingKey);
                // 发送消息，只要不抛异常就是成功。
                if (sendResult != null) {
                    System.out.println(new Date() + " Send mq message success. Topic is:" + msg.getTopic() + " msgId is: " + sendResult.getMessageId());
                }
            }
            catch (Exception e) {
                // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                System.out.println(new Date() + " Send mq message failed. Topic is:" + msg.getTopic());
                e.printStackTrace();
            }
        }
        // 在应用退出前，销毁Producer对象。
        // 注意：如果不销毁也没有问题。
        producer.shutdown();
    }
}
