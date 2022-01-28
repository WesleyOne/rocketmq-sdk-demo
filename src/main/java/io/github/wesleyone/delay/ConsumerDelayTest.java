package io.github.wesleyone.delay;

import java.util.Properties;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import io.github.wesleyone.MqConfig;

/**
 * Push方式
 */
public class ConsumerDelayTest {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 您在控制台创建的Group ID。
        properties.put(PropertyKeyConst.GROUP_ID, "DelayGroupId");
        // AccessKeyKey、SecretKey授权相关。若启用需要配置,
        // 参考https://wesleyone.github.io/rocketmqCake/best_practice/acl/user_guide.html
        properties.put(PropertyKeyConst.AccessKey, MqConfig.ACCESS_KEY);
        properties.put(PropertyKeyConst.SecretKey, MqConfig.SECRET_KEY);
        // 设置TCP接入域名，即nameserver地址
        properties.put(PropertyKeyConst.NAMESRV_ADDR, MqConfig.NAMESRV_ADDR);

        // 集群订阅方式(默认)。
        // properties.put(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        // 广播订阅方式。
        // properties.put(PropertyKeyConst.MessageModel, PropertyValueConst.BROADCASTING);

        Consumer consumer = ONSFactory.createConsumer(properties);
        consumer.subscribe("DelayTopicTestMQ", "TagA||TagB", new MessageListener() { //订阅多个Tag。
            public Action consume(Message message, ConsumeContext context) {
                System.out.println("checkCt:"+System.currentTimeMillis()+",ct:"+new String(message.getBody())+",Receive: " + message);
                return Action.CommitMessage;
            }
        });

        consumer.start();
        System.out.println("Consumer Started");
    }
}
