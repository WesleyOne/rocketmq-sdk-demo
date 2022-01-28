package io.github.wesleyone.transaction;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;

/**
 * 6.检查本地事务的状态
 */
public class LocalTransactionCheckerImpl implements LocalTransactionChecker {
    @Override
    public TransactionStatus check(Message msg) {
        System.out.println("msgID:"+msg.getMsgID());
        System.out.println("LocalTransactionCheckerImpl.check:"+msg);
        return TransactionStatus.CommitTransaction;
    }
}
