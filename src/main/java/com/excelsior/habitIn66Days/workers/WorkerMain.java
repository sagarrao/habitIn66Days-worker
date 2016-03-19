package com.excelsior.habitIn66Days.workers;


import com.excelsior.habitIn66Days.constants.Constants;
import com.excelsior.habitIn66Days.schedulers.SchedulerMain;
import com.excelsior.habitIn66Days.utils.SendEmailUtil;

import com.rabbitmq.client.QueueingConsumer;
import org.apache.commons.lang.SerializationUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

public class WorkerMain {

    private final static Logger logger = LoggerFactory.getLogger(WorkerMain.class);

    public static void main(String[] args) throws Exception {

        SchedulerMain.initConnection();
        QueueingConsumer consumer = new QueueingConsumer(SchedulerMain.channel);
        logger.info("SchedulerMain.channel--->"+SchedulerMain.channel);
        SchedulerMain.channel.basicConsume(Constants.enrolledUsersQueue, false, consumer);
       
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(); 
            if (delivery != null) {
                Document document = (Document) SerializationUtils.deserialize(delivery.getBody());
                Observable.just(document)
                        .subscribeOn(Schedulers.io())
                        .subscribe(d -> {
                            SendEmailUtil.dispatchForEmail(d);
                        });
                SchedulerMain.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        }

    }

}
