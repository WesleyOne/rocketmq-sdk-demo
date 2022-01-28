package io.github.wesleyone.normal.async;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.OnExceptionContext;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendCallback;
import com.aliyun.openservices.ons.api.SendResult;
import io.github.wesleyone.MqConfig;

/**
 * 异步发送
 */
public class ProducerAsyncTest {
    public static void main(String[] args) throws InterruptedException {
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

        for (int i=0;i<10;i++) {
            Message msg = new Message(
                    // 普通消息所属的Topic，切勿使用普通消息的Topic来收发其他类型的消息。
                    "NormalAsyncTopicTest",
                    // Message Tag，可理解为Gmail中的标签，对消息进行再归类，方便Consumer指定过滤条件在消息队列RocketMQ版的服务器过滤。
                    "TagA",
                    // Message Body，任何二进制形式的数据，消息队列RocketMQ版不做任何干预，需要Producer与Consumer协商好一致的序列化和反序列化方式。
                    ("Hello MQ.ProducerAsyncTest"+i).getBytes());

            // 设置代表消息的业务关键属性，请尽可能全局唯一。 以方便您在无法正常收到消息情况下，可通过消息队列RocketMQ版控制台查询消息并补发。
            // 注意：不设置也不会影响消息正常收发。
            msg.setKey("ORDERID_"+i);

            // 异步发送消息, 发送结果通过callback返回给客户端。
            producer.sendAsync(msg, new SendCallback() {
                @Override
                public void onSuccess(final SendResult sendResult) {
                    // 消息发送成功。
                    System.out.println("send message success. topic=" + sendResult.getTopic() + ", msgId=" + sendResult.getMessageId());
                }

                @Override
                public void onException(OnExceptionContext context) {
                    // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
                    System.out.println("send message failed. topic=" + context.getTopic() + ", msgId=" + context.getMessageId());
                }
            });
        }

        // 阻塞当前线程3秒，等待异步发送结果。
        TimeUnit.SECONDS.sleep(3);

        // 在应用退出前，销毁Producer对象。注意：如果不销毁也没有问题。
        producer.shutdown();
    }
}
