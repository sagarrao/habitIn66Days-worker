package com.excelsior.habitIn66Days.schedulers;

import com.excelsior.habitIn66Days.constants.Constants;
import com.excelsior.habitIn66Days.utils.SendEmailUtil;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.time.LocalDate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by RaoSa on 2/14/2016.
 */
public class FetchForAdminNotificationJob implements Job {

    private StringBuffer message = new StringBuffer();

    private final String daysDelimiter = "\n\n" + "#########################################################" + "\n\n";

    private final String usersDelimiter = "\n\n" + "*********************************************************" + "\n\n";

    private final static Logger logger = LoggerFactory.getLogger(FetchForAdminNotificationJob.class);

    private Func1<Document, Boolean> userToBeFollowedUp = (document) -> {
        LocalDate programStartLocalDate = Constants.localDateFromDate.apply(document.getDate("habitsProgramEnrollmentDate"));
        int dayNumber = Constants.getDateDifference.apply(programStartLocalDate);
        return Constants.followupDays.contains(++dayNumber) ? true : false;
    };


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        ConcurrentHashMap<Integer, CopyOnWriteArrayList<Document>> usersToBeNotifiedAbout = new ConcurrentHashMap<>();

        FindIterable<Document> iterable = Constants.db.getCollection("genius").find(eq("enrolledForHabitIn66Days", true));
        Observable.from(iterable)
                .filter(userToBeFollowedUp)
                .scan(usersToBeNotifiedAbout, ((map, document) -> {
                    try {
                        LocalDate programStartLocalDate = Constants.localDateFromDate.apply(document.getDate("habitsProgramEnrollmentDate"));
                        AtomicInteger dayNumber = new AtomicInteger(Constants.getDateDifference.apply(programStartLocalDate));
                        map.computeIfAbsent(dayNumber.incrementAndGet(), list -> new CopyOnWriteArrayList<>()).add(document);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return map;
                }))
                .takeLast(1)
                .subscribeOn(Schedulers.computation())
                .subscribe(
                        m -> {
                            composeEmailMessage(m);
                            SendEmailUtil.sendEmail(message);
                        });
    }

    private void composeEmailMessage(
            ConcurrentHashMap<Integer, CopyOnWriteArrayList<Document>> usersFollowUpMap) {
        try {
            if (usersFollowUpMap.isEmpty())
                message.append("A happy day today.. No follow ups!!");
            else {
				logger.info("usersFollowUpMap::"+usersFollowUpMap);
                usersFollowUpMap.forEach((dayNumber, documents) -> {
                    message = message.append("Day # " + dayNumber
                            + " follow ups " + "\n\n");
                    documents.forEach(document -> {
                        Document nameDoc = (Document) (document.get("name"));
                        String name = nameDoc.getString("first").concat(" ").concat(nameDoc.getString("last"));
                        message.append("Name:"
                                + name
                                + "\n\n"
                                + "Habbit Working Upon:"
                                + document.getString("habitWorkingUpn")
                                + "\n\n"
                                + "Phone #"
                                + Long.toString(document.getLong("phoneNumber"))
                                +"\n\n"
                                + usersDelimiter);
                    });
                    message.append(daysDelimiter);
                });
            }
            logger.info("Email message for follow up mail::"+message.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
