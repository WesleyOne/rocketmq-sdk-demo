package io.github.wesleyone.order;
import java.util.Properties;


import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.order.ConsumeOrderContext;
import com.aliyun.openservices.ons.api.order.MessageOrderListener;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import io.github.wesleyone.MqConfig;

/**
 * 顺序消息消费
 */
public class OrderConsumerClient {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 您在消息队列RocketMQ版控制台创建的Group ID。
        properties.put(PropertyKeyConst.GROUP_ID, "OrderGroupId");
        // AccessKeyKey、SecretKey授权相关。若启用需要配置,
        // 参考https://wesleyone.github.io/rocketmqCake/best_practice/acl/user_guide.html
        properties.put(PropertyKeyConst.AccessKey, MqConfig.ACCESS_KEY);
        properties.put(PropertyKeyConst.SecretKey, MqConfig.SECRET_KEY);
        // 设置TCP接入域名，即nameserver地址
        properties.put(PropertyKeyConst.NAMESRV_ADDR, MqConfig.NAMESRV_ADDR);
        // 顺序消息消费失败进行重试前的等待时间，单位（毫秒），取值范围: 10毫秒~30,000毫秒。
        properties.put(PropertyKeyConst.SuspendTimeMillis,"100");
        // 消息消费失败时的最大重试次数。
        properties.put(PropertyKeyConst.MaxReconsumeTimes,"20");

        // 在订阅消息前，必须调用start方法来启动Consumer，只需调用一次即可。
        OrderConsumer consumer = ONSFactory.createOrderedConsumer(properties);

        consumer.subscribe(
                // Message所属的Topic。
                "Order_global_topic",
                // 订阅指定Topic下的Tags：
                // 1. * 表示订阅所有消息。
                // 2. TagA || TagB || TagC表示订阅TagA或TagB或TagC的消息。
                "*",
                new MessageOrderListener() {
                    /**
                     * 1. 消息消费处理失败或者处理出现异常，返回OrderAction.Suspend。
                     * 2. 消息处理成功，返回OrderAction.Success。
                     */
                    @Override
                    public OrderAction consume(Message message, ConsumeOrderContext context) {
                        System.out.println(message);
                        return OrderAction.Success;
                    }
                });

        consumer.start();
    }
}
