package com.excelsior.habitIn66Days.constants;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

import java.time.*;

import java.util.Arrays;
import java.util.Date;

import java.util.List;
import java.util.function.Function;

/**
 * Created by RaoSa on 2/3/2016.
 */
public class Constants {

    public final static String enrolledUsersQueue = "enrolled-users-queue";
    public final static String mongoDb = "heroku_wx60bphv";
//    public final static String mandrillApiKey = "uP9avYSTtIVL4pvpo6erEg"; // Sagar's api key
//    public final static String mandrillApiKey = "FTTlKeDFpCvLx6aWmNVCSQ";
    public final static String sendGridApiKey = System.getenv("SENDGRID_API_KEY");
    public final static String mongoClientURI = System.getenv("MONGO_CLIENT_URI");
    public final static String fromEmail = "anuraggupta86@gmail.com";
    public final static String fromName = "Anurag Gupta";
    public final static MongoClientURI clientUri = new MongoClientURI(mongoClientURI);
    public final static MongoClient mongoClient = new MongoClient(clientUri);
    public final static MongoDatabase db = mongoClient.getDatabase(mongoDb);
    public final static String followUpSubject = "Habits Program Followups";
    public final static String anuragEmail = "anuraggupta86@gmail.com";
    public final static String anuragName = "Anurag";

    public final static Function<LocalDate,Integer> getDateDifference = (date) -> Period.between(date, LocalDate.now()).getDays();

    public final static Function<Date,LocalDate> localDateFromDate = (date -> {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
                .toLocalDate();
    });

    public final static List<Integer> followupDays = Arrays.asList(new Integer[]{1,7,21,45,66});

}
