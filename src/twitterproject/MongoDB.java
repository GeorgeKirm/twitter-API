/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.Document;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static java.lang.Thread.sleep;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author url
 */
public class MongoDB {

    private MongoDatabase db;
    private MongoCollection<Document> collection;
    private MongoClient mongo;

    MongoDB() {
        try {
            //initialize MongoDB, set configuration and load collection
            System.out.println("Connecting to Mongo DB..");
            mongo = new MongoClient();
            db = mongo.getDatabase("tweetsDBT");

            collection = db.getCollection("tweetsCollection");
            sleep(300); // delete me
            System.out.println("Connected to Mongo DB!");
        } catch (MongoException ex) {
            System.out.println("MongoException : " + ex.getMessage());
        } catch (InterruptedException ex) { //delete me
            Logger.getLogger(MongoDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public MongoCollection<Document> collectionGetter() {
        return collection;
    }

    public MongoDatabase dataBaseGetter() {
        return db;
    }

    public Mongo mongoGetter() { // Not useable for the moment, delete if not needed
        return mongo;
    }

    public void readDataBase() {
        // read from the database and print
        System.out.println("Reading database:");

        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            int i = 0;
            while (cursor.hasNext()) {
                i++;
                System.out.println(i);
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        System.out.println("Fisnish reading database!");
    }

    public void closeDataBase() {
        System.out.println("Closing connection to Mongo DB..");
        this.mongo.close();
        System.out.println("Connection to Mongo DB closed!");
    }

    public void dropCollection() {
        System.out.println("Droping collection..");
        this.collection.drop();
        System.out.println("Collection droped!");
    }

    public void showTweetsCount() {
        System.out.println("Number of tweets: " + this.collectionGetter().count());
    }

    public void test() {
        // read from the database and print
        System.out.println("Test starts:");

        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            int i = 0;
            while (cursor.hasNext()) {
                i++;
                Document thisCursor = cursor.next();
                //System.out.println(thisCursor); //prints full json format

                String text = thisCursor.get("text").toString(); //text of the tweet

                if (!text.contains("…")) { //if the text has ellipsis, ignore it
                    System.out.println("keimeno ok:");
                    String timestamp = thisCursor.get("timestamp_ms").toString();
                    System.out.println("Timestamp: " + timestamp);
                    System.out.println("Keimeno: " + text);
                    Document userField = (Document) (thisCursor.get("user"));
                    String username = (String) userField.get("screen_name");
                    System.out.println("Onoma: " + username);
                    Document entitiesField = (Document) (thisCursor.get("entities"));
                    Document retweetedStatus = (Document) thisCursor.get("retweeted_status");
                    System.out.println("Retweeted Status: " + retweetedStatus);
                    if (retweetedStatus != null) {
                        String idStr = (String) retweetedStatus.get("id_str");
                        System.out.println("Id String: " + idStr);
                    }
                    Matcher matcher = Pattern.compile("#(\\w+)").matcher(text);
                    Matcher matcher2 = Pattern.compile("@(\\w+)").matcher(text);
                    System.out.println("Hashtags!");
                    while (matcher.find()) {
                        System.out.println(matcher.group(1));
                    }
                    System.out.println("Mentions!");
                    while (matcher2.find()) {
                        System.out.println(matcher2.group(1));
                    }
                    //System.out.println("prwto");
                    //System.out.println(entitiesField); //to value tou entities
                    //System.out.println("deutero");
                    //System.out.println(entitiesField.get("urls")); //to value tou urls
                    ArrayList urlsField = (ArrayList) entitiesField.get("urls");
                    //System.out.println("trito");
                    int z = urlsField.size();
                    int j;
                    if (z > 0) {
                        ArrayList<String> a = new ArrayList();
                        ArrayList<String> b = new ArrayList();
                        for (j = 0; j < z; j++) {
                            Document currentUrlsField = (Document) urlsField.get(j);
                            //System.out.println(currentUrlsField);
                            //System.out.println("tetarto");
                            String url = (String) currentUrlsField.get("url");
                            if (!url.equals("")) {
                                a.add(j, url);
                                System.out.println("Url: " + url);
                            }
                            String expandedUrl = (String) currentUrlsField.get("expanded_url");
                            if (expandedUrl != null) {
                                b.add(j, expandedUrl);
                                System.out.println("Expanded Url: " + expandedUrl);
                            }

                        }
                    }
                    System.out.println(i);
                }
            }
        }
        System.out.println("Test ends!");
    }
}
