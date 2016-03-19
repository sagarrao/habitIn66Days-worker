package com.excelsior.habitIn66Days.schedulers;

import com.excelsior.habitIn66Days.constants.Constants;
import com.excelsior.habitIn66Days.utils.SendEmailUtil;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

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

    private final String daysDelimiter = "\n" + "#########################################################" + "\n";

    private final String usersDelimiter = "\n" + "*********************************************************" + "\n";

    private Func1<Document, Boolean> userToBeFollowedUp = (document) -> {
        LocalDate programStartLocalDate = Constants.localDateFromDate.apply(document.getDate("habitsProgramEnrollmentDate"));
        int dayNumber = Constants.getDateDifference.apply(programStartLocalDate);
        return Constants.followupDays.contains(dayNumber) ? true : false;
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
                        map.computeIfAbsent(dayNumber.get(), list -> new CopyOnWriteArrayList<Document>()).add(document);
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
                usersFollowUpMap.forEach((dayNumber, documents) -> {
                    message = message.append("Day # " + dayNumber
                            + " follow ups " + "\n");
                    documents.forEach(document -> {
                        Document nameDoc = (Document) (document.get("name"));
                        String name = nameDoc.getString("first").concat(" ").concat(nameDoc.getString("last"));
                        message.append("Name:"
                                + name
                                + "\n"
                                + "Habbit Working Upon:"
                                + document.getString("habitWorkingUpn")
                                + "\n"
                                + "Phone #"
                                + document.getLong("phoneNumber")
                                + usersDelimiter);
                    });
                    message.append(daysDelimiter);
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
