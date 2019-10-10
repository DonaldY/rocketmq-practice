package com.donaldy.demo.standard;

import com.donaldy.demo.transaction.TransactionListenerImpl;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

@Component
public class TransactionProducer implements InitializingBean {

    @Autowired
    private TransactionMQProducer producer;

    @Autowired
    private ExecutorService executorService;

    @Autowired
    private TransactionListenerImpl transactionListenerImpl;

    private static final String NAME_SERVER = "";

    private static final String PRODUCER_GROUP_NAME = "tx_pay_producer_group_name";

    private TransactionProducer() {
        this.producer = new TransactionMQProducer(PRODUCER_GROUP_NAME);
        this.executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {

                Thread thread = new Thread(runnable);
                thread.setName(PRODUCER_GROUP_NAME + "-check-thread");
                return thread;
            }
        });

        this.producer.setExecutorService(executorService);
        this.producer.setNamesrvAddr(NAME_SERVER);
    }

    @Override
    public void afterPropertiesSet() {

        this.producer.setTransactionListener(transactionListenerImpl);

        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {

        this.producer.shutdown();
    }

    public TransactionSendResult sendMessage(Message message, Object argument) {
        TransactionSendResult sendResult = null;
        try {
            sendResult = this.producer.sendMessageInTransaction(message, argument);
        } catch (Exception e) {

            e.printStackTrace();
        }

        return sendResult;
    }


}
