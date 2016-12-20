/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

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
    private MongoCollection<Document> collectionXima;
    private MongoClient mongo;

    MongoDB() {
        try {
            //initialize MongoDB, set configuration and load collection
            System.out.println("Connecting to Mongo DB..");
            mongo = new MongoClient();
            db = mongo.getDatabase("tweetsDBT");

            collection = db.getCollection("tweetsCollection");
            collectionXima = db.getCollection("tweetsCollectionTest");
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

    public void readCollection(int a) {
        // read from the database and print
        System.out.println("Reading collection:");

        MongoCursor<Document> cursor = null;
        switch (a) {
            case 1:
                cursor = collection.find().iterator();
                break;
            case 2:
                cursor = collectionXima.find().iterator();
                break;
            default:
                System.out.println("Error: No such collection! (You need to give a number for the collection like 1 or 2)");
                break;
        }
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
        System.out.println("Finish reading collection!");
    }
    
    public void closeDataBase() {
        System.out.println("Closing connection to Mongo DB..");
        this.mongo.close();
        System.out.println("Connection to Mongo DB closed!");
    }

    public void dropCollection(int a) {
        System.out.println("Droping collection..");
        switch (a) {
            case 1:
                this.collection.drop();
                break;
            case 2:
                this.collectionXima.drop();
                break;
            default:
                System.out.println("Error: No such collection! (You need to give a number for the collection like 1 or 2)");
                break;
        }
        System.out.println("Collection droped!");
    }

    public void showTweetsCount() {
        System.out.println("Number of tweets: " + this.collectionGetter().count());
    }

    public void test(String loula) {
        Document doc = Document.parse(loula);
        collectionXima.insertOne(doc);
    }

    public void test() {
        // read from the database and print
        System.out.println("Test starts:");

        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            int i = 0;
            while (cursor.hasNext()) {
                System.out.println(i);
                i++;
                Document thisCursor = cursor.next();
                //System.out.println(thisCursor); //prints full json format

                String text = thisCursor.get("text").toString(); //text of the tweet
                String forXima = "{";
                if (!text.contains("…")) { //if the text has ellipsis, ignore it
                    System.out.println("keimeno ok:");
                    String timestamp = thisCursor.get("timestamp_ms").toString();
                    forXima = forXima + "\nTimestamp: \"" + timestamp + "\"";
//                    System.out.println("Keimeno: " + text);
                    Document userField = (Document) (thisCursor.get("user"));
                    String username = (String) userField.get("screen_name");
                    forXima = forXima + "\nOnoma: \"" + username + "\"";
                    Document entitiesField = (Document) (thisCursor.get("entities"));
                    Document retweetedStatus = (Document) thisCursor.get("retweeted_status");
                   // forXima = forXima + "\nRetweeted_Status: \"" + retweetedStatus + "\"";
                    if (retweetedStatus != null) {
                        String idStr = (String) retweetedStatus.get("id_str");
                        forXima = forXima + "\nId_String: " + idStr;
                    }
                    Matcher matcher = Pattern.compile("#(\\w+)").matcher(text);
                    Matcher matcher2 = Pattern.compile("@(\\w+)").matcher(text);
                    forXima = forXima + "\nHashtags: [";
                    boolean iCheck1 = true;
                    boolean iCheck2 = true;
                    while (matcher.find()) {
                        if (iCheck1 == true) {
                            forXima = forXima  + "\"" + matcher.group(1) + "\"";
                            iCheck1 = false;
                        } else {
                            forXima = forXima  + ", " + "\"" + matcher.group(1) + "\"";
                        }
                    }
                    forXima = forXima + "]";
                    forXima = forXima + "\nMentions: [";
                    while (matcher2.find()) {
                        if (iCheck2 == true) {
                            forXima = forXima + "\"" + matcher2.group(1) + "\"";
                            iCheck2 = false;
                        } else {
                            forXima = forXima + ", " + "\"" + matcher2.group(1) + "\"";
                        }
                    }
                    forXima = forXima + "]";
                    ArrayList urlsField = (ArrayList) entitiesField.get("urls");
                    int z = urlsField.size();
                    int j;
                    String urls = "\nURLs: [";
                    String exUrls = "\nexURLs: [";
                    iCheck1 = true;
                    iCheck2 = true;
                    if (z > 0) {
                        ArrayList<String> a = new ArrayList();
                        ArrayList<String> b = new ArrayList();
                        for (j = 0; j < z; j++) {
                            Document currentUrlsField = (Document) urlsField.get(j);
                            String url = (String) currentUrlsField.get("url");
                            if (!url.equals("")) {
                                a.add(j, url);
                                if (iCheck1 == true) {
                                    urls = urls + "\"" + url + "\"" ;
                                    iCheck1 = false;
                                } else {
                                    urls = urls + ", " + "\"" + url + "\"" ;
                                }
                            }
                            String expandedUrl = (String) currentUrlsField.get("expanded_url");
                            if (expandedUrl != null) {
                                b.add(j, expandedUrl);
                                if (iCheck2 == true) {
                                    exUrls = exUrls + "\"" + expandedUrl + "\"" ;
                                    iCheck2 = false;
                                } else {
                                    exUrls = exUrls + ", " + "\"" + expandedUrl + "\"" ;
                                }
                            }
                        }
                        forXima = forXima + urls + "]" + exUrls + "]";
                    }
                    forXima = forXima + "\n}";

                    System.out.println(forXima);
                    test(forXima);
                }
            }
        }
        System.out.println("Test ends!");
    }
}
