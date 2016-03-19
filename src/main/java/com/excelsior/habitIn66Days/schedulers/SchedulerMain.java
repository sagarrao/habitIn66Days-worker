package com.excelsior.habitIn66Days.schedulers;

import com.excelsior.habitIn66Days.constants.Constants;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.lang.SerializationUtils;
import org.bson.Document;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.model.Filters.eq;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.repeatSecondlyForever;
import static org.quartz.TriggerBuilder.newTrigger;

public class SchedulerMain {

    private final static Logger logger = LoggerFactory.getLogger(SchedulerMain.class);
    public static ConnectionFactory factory;
    public static Connection connection;
    public static Channel channel;

    public static void initConnection(){
        try {
            factory = new ConnectionFactory();
            factory.setUri(System.getenv("CLOUDAMQP_URL"));
            connection = factory.newConnection();
            channel = connection.createChannel();
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("x-ha-policy", "all");
            channel.queueDeclare(Constants.enrolledUsersQueue, true, false, false, params);
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException | URISyntaxException | TimeoutException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception {
        initConnection();
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        scheduler.start();
        JobDetail jobDetail = newJob(PersistUsersInQueue.class).build();
        JobDetail followUpJob = newJob(FetchForAdminNotificationJob.class).build();

        //TODO Use this cron for daily once email shooting:
       /* Trigger trigger = newTrigger()
                .startNow()
                .withSchedule(CronScheduleBuilder.cronSchedule("0 15 10 ? * *")) Shoots at 10.15 AM daily. Adjust accordingly...
                .build();*/

        Trigger trigger = newTrigger()
                .startNow()
                .withSchedule(repeatSecondlyForever(300))
                .build();

        Trigger followUpTrigger = newTrigger()
                .startNow()
                .withSchedule(repeatSecondlyForever(300))
                .build();

        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.scheduleJob(followUpJob, followUpTrigger);
    }

    public static class PersistUsersInQueue implements Job {

        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            try {
                FindIterable<Document> iterable = Constants.db.getCollection("genius").find(eq("enrolledForHabitIn66Days", true));
                iterable.forEach((Block<Document>) document -> {
                    String msg = "Sent at:" + System.currentTimeMillis();
                    byte[] bytes = SerializationUtils.serialize(document);
                    try {
                        if(!connection.isOpen()){
                            connection = factory.newConnection();
                            channel = connection.createChannel();
                        }
                        channel.basicPublish("", Constants.enrolledUsersQueue, MessageProperties.PERSISTENT_TEXT_PLAIN, bytes);
                        logger.info("Message published to queue: " + msg);
                    } catch (IOException | TimeoutException e) {
                        e.printStackTrace();
                    }
                });
            }
            catch (Exception e) {
                logger.error(e.getMessage(), e);
            }finally {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
