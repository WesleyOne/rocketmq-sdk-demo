package io.github.wesleyone.transaction;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionExecuter;
import com.aliyun.openservices.ons.api.transaction.TransactionProducer;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;
import io.github.wesleyone.MqConfig;

/**
 * 事务消息-正常提交
 */
public class TransactionProducerClientCommit {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        // AccessKeyKey、SecretKey授权相关。若启用需要配置,
        // 参考https://wesleyone.github.io/rocketmqCake/best_practice/acl/user_guide.html
        properties.put(PropertyKeyConst.AccessKey, MqConfig.ACCESS_KEY);
        properties.put(PropertyKeyConst.SecretKey, MqConfig.SECRET_KEY);
        // 设置TCP接入域名，即nameserver地址
        properties.put(PropertyKeyConst.NAMESRV_ADDR, MqConfig.NAMESRV_ADDR);

        TransactionProducer producer = ONSFactory.createTransactionProducer(properties,
                new LocalTransactionCheckerImpl());
        producer.start();
        Message msg = new Message("TransactionTopic","TagA","Hello MQ transaction===commit".getBytes());
        try {
            SendResult sendResult = producer.send(msg, new LocalTransactionExecuter() {
                @Override
                public TransactionStatus execute(Message msg, Object arg) {
                    // 消息ID（有可能消息体一样，但消息ID不一样，当前消息属于半事务消息，所以消息ID在消息队列RocketMQ版控制台无法查询）。
                    String msgId = msg.getMsgID();
                    System.out.println("msg:"+msg);
                    // 如果业务本身是幂等的，可以忽略，否则需要利用msgId或crc32Id来做幂等。
                    // 如果要求消息绝对不重复，推荐做法是对消息体使用crc32或MD5来防止重复消息。
                    TransactionStatus transactionStatus = TransactionStatus.CommitTransaction;
                    System.out.println("Message Id:"+msgId+",transactionStatus:"+transactionStatus.name());
                    return transactionStatus;
                }
            }, null);
        }
        catch (Exception e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
            System.out.println(new Date() + " Send mq message failed. Topic is:" + msg.getTopic());
            e.printStackTrace();
        }
        // demo example防止进程退出（实际使用不需要这样）。
        TimeUnit.MILLISECONDS.sleep(Integer.MAX_VALUE);
    }
}
