package com.excelsior.habitIn66Days.utils;

import com.excelsior.habitIn66Days.constants.Constants;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import com.sendgrid.SendGrid;
import com.sendgrid.SendGridException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by RaoSa on 2/13/2016.
 */
public class SendEmailUtil {

    private final static Logger logger = LoggerFactory.getLogger(SendEmailUtil.class);

    private static final SendGrid sendGrid = new SendGrid (Constants.sendGridApiKey);

    private static final SendGrid.Email composeMessage(String name,String recipientEmailId,Document document){
        SendGrid.Email email = new SendGrid.Email();
        email.setSubject(document.getString("Subject"));
        email.setFrom(Constants.fromEmail);
        email.setFromName(Constants.fromName);
        email.addTo(recipientEmailId);
        String body = "Hi ".concat(name).concat("\n\n").concat(document.getString("Body"));
        email.setText(body);
        return email;
    }

    private static final SendGrid.Email composeMessage(String name,String recipientEmailId,String message){
        SendGrid.Email email = new SendGrid.Email();
        email.setSubject(Constants.followUpSubject);
        email.setFrom(Constants.fromEmail);
        email.setFromName(Constants.fromName);
        email.addTo(recipientEmailId);
        email.setText(message);
        return email;
    }

    public static void sendEmail(StringBuffer message){
        SendGrid.Email email = composeMessage(Constants.anuragName,"sagarmeansocean@gmail.com", message.toString());
        SendGrid.Response response = null;
        try {
            response = sendGrid.send(email);
        } catch (SendGridException e) {
            e.printStackTrace();
        }
        logger.info("email sent to-->" + Constants.anuragEmail);
        System.out.println("Response status--->" + response.getMessage() );
    }

    private static void sendEmail(int dayNumber, String name, String recipientEmailId,Optional<Boolean> habitsProgramNotifiedForFirstTimeOpt){

        logger.info("Sending email for-->"+name+" dayNumber--->"+dayNumber);
        //TODO Ideally this should be cached.. But right now this isn't that big a bottleneck so retaining it...
        FindIterable<Document> iterable = Constants.db.getCollection("emails").find(eq("day", Integer.toString(dayNumber)));
        iterable.forEach((Block<Document>) document -> {
        try {
            if (dayNumber == 1 && (!habitsProgramNotifiedForFirstTimeOpt.isPresent() || habitsProgramNotifiedForFirstTimeOpt.get())) {
                logger.info("No email sent as the user has already been notified about his 1st day");
                return;
            }
            SendGrid.Email email = composeMessage(name,recipientEmailId,document);
            SendGrid.Response response = sendGrid.send(email);
            System.out.println("Response status--->"+response.getMessage());
        }
        catch (Exception e){
            logger.error("Exception thrown");
            e.printStackTrace();
        }});
    }

    public static void dispatchForEmail(Document document){
        String emailId = document.get("email").toString();
        Document nameDoc = (Document)(document.get("name"));
        String name = nameDoc.getString("first").concat(" ").concat(nameDoc.getString("last"));
        Optional<Boolean> habitsProgramNotifiedForFirstTimeOpt = Optional.ofNullable(document.getBoolean("habitsProgramNotifiedForFirstTime"));
        LocalDate programStartLocalDate = Constants.localDateFromDate.apply(document.getDate("habitsProgramEnrollmentDate"));
        int dayNumber = Constants.getDateDifference.apply(programStartLocalDate);
        sendEmail(++dayNumber,name,emailId,habitsProgramNotifiedForFirstTimeOpt);
    }
}
