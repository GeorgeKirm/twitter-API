/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import com.mongodb.BasicDBObject;
import org.bson.Document;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static java.lang.Thread.sleep;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private List <String> pedia;

    MongoDB() {
        pedia = new ArrayList<>();
        pedia.add("Timestamp");
        pedia.add("Id_String");
        pedia.add("Hashtags");
        pedia.add("Mentions");
        pedia.add("URLs");
        pedia.add("exURLs");
        try {
            //initialize MongoDB, set configuration and load collection
            System.out.println("Connecting to Mongo DB..");
            mongo = new MongoClient();
            db = mongo.getDatabase("tweetsDB");

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

    public void showTweetsCount(int a) {
        switch (a) {
            case 1:
                System.out.println("Number of tweets: " + this.collection.count());
                break;
            case 2:
                System.out.println("Number of tweets: " + this.collectionXima.count());
                break;
            default:
                System.out.println("Error: No such collection! (You need to give a number for the collection like 1 or 2)");
                break;
        }

    }

    public void elegxosIndexing() {
        
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("Onoma", "ShaneinCurry");
        collectionXima.distinct ("Hashtags", null);
//        ListIndexesIterable indexes = collectionXima.listIndexes(BasicDBObject.class);
//        MongoCursor cursor3 = indexes.iterator();
//        while (cursor3.hasNext()) {
//            BasicDBObject getCreatedIdx = (BasicDBObject) cursor3.next();
//            System.out.println("index is: " + getCreatedIdx.toString());
//        }
//        System.out.println("Single search!");
//        BasicDBObject whereQuery = new BasicDBObject();
//        whereQuery.put("Onoma", "PostSexyLips");
//        System.out.println(collectionXima.find(whereQuery).modifiers(new Document("$explain", true)).first());
    }

    private void test(String pedio, String username, String loula) {
        loula = loula.replaceAll("\"", "");
        if (loula.equals("")) {
        } else {
            List<String> voulaTimestamp = Arrays.asList(loula.split("\\s*,\\s*"));
            collectionXima.updateOne(new Document("Onoma", username),
                    new Document("$pushAll", new Document(pedio, voulaTimestamp)));
        }
    }
    
    private void test(String timestamp, String username, String idStr, String hashtagsS, String mensionsS, String urls, String exUrls) {
//        if (checkerFirst) {
//            BasicDBObject index = new BasicDBObject("Onoma", 1);
//            collectionXima.createIndex(index);
//            checkerFirst = false;
//        }
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("Onoma", username);
        MongoCursor<Document> cursorLoula = collectionXima.find(whereQuery).iterator();
        if (cursorLoula.hasNext()) {
            test(pedia.get(0), username, timestamp);
            test(pedia.get(1), username, idStr);
            test(pedia.get(2), username, hashtagsS);
            test(pedia.get(3), username, mensionsS);
            test(pedia.get(4), username, urls);
            test(pedia.get(5), username, exUrls);
        } else {
            String forXima = "{";
            forXima = forXima + "\nTimestamp: [" + timestamp + "]";
            forXima = forXima + "\nOnoma: \"" + username + "\"";
            if (idStr == null) {
                forXima = forXima + "\nId_String: []";
            } else {
                forXima = forXima + "\nId_String: [" + idStr + "]";
            }
            forXima = forXima + "\nHashtags: [" + hashtagsS + "]";
            forXima = forXima + "\nMentions: [" + mensionsS + "]";
            forXima = forXima + "\nURLs: [" + urls + "]" + "\nexURLs: [" + exUrls + "]";
            forXima = forXima + "\n}";
            Document doc = Document.parse(forXima);
            collectionXima.insertOne(doc);
        }

    }

    public void test() {
        // read from the database and print
        System.out.println("Test starts:");
        dropCollection(2);
        BasicDBObject index = new BasicDBObject("Onoma", 1);
        collectionXima.createIndex(index);
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
//            int i = 0;
            while (cursor.hasNext()) {
//            while (cursor.hasNext() && i <= 1000) {
//                System.out.println(i);
//                i++;
                Document thisCursor = cursor.next();
                //System.out.println(thisCursor); //prints full json format

                String text = thisCursor.get("text").toString(); //text of the tweet
                    
                if (!text.contains("…")) { //if the text has ellipsis, ignore it
//                    System.out.println("keimeno ok:");
                    String timestamp = thisCursor.get("timestamp_ms").toString();
//                    System.out.println("Keimeno: " + text);
                    Document userField = (Document) (thisCursor.get("user"));
                    String username = (String) userField.get("screen_name");
                    Document retweetedStatus = (Document) thisCursor.get("retweeted_status");
                    String idStr = "";
                    if (retweetedStatus != null) {
                        idStr = (String) retweetedStatus.get("id_str");
                        idStr = "\"" + idStr + "\"";
                    }
                    Matcher matcher = Pattern.compile("#(\\w+)").matcher(text);
                    Matcher matcher2 = Pattern.compile("@(\\w+)").matcher(text);
                    boolean iCheck1 = true;
                    boolean iCheck2 = true;
                    String hashtagsS = "";
                    while (matcher.find()) {
                        if (iCheck1 == true) {
                            hashtagsS = "\"" + matcher.group(1) + "\"";
                            iCheck1 = false;
                        } else {
                            hashtagsS = hashtagsS + ", " + "\"" + matcher.group(1) + "\"";
                        }
                    }
                    String mensionsS = "";
                    while (matcher2.find()) {
                        if (iCheck2 == true) {
                            mensionsS = "\"" + matcher2.group(1) + "\"";
                            iCheck2 = false;
                        } else {
                            mensionsS = mensionsS + ", " + "\"" + matcher2.group(1) + "\"";
                        }
                    }
                    Document entitiesField = (Document) (thisCursor.get("entities"));
                    ArrayList urlsField = (ArrayList) entitiesField.get("urls");
                    int z = urlsField.size();
                    int j;
                    String urls = "";
                    String exUrls = "";
                    iCheck1 = true;
                    iCheck2 = true;
                    if (z > 0) {
//                        ArrayList<String> a = new ArrayList();
//                        ArrayList<String> b = new ArrayList();
                        for (j = 0; j < z; j++) {
                            Document currentUrlsField = (Document) urlsField.get(j);
                            String url = (String) currentUrlsField.get("url");
                            if (!url.equals("")) {
//                                a.add(j, url);
                                if (iCheck1 == true) {
                                    urls = "\"" + url + "\"";
                                    iCheck1 = false;
                                } else {
                                    urls = urls + ", " + "\"" + url + "\"";
                                }
                            }
                            String expandedUrl = (String) currentUrlsField.get("expanded_url");
                            if (expandedUrl != null) {
//                                b.add(j, expandedUrl);
                                if (iCheck2 == true) {
                                    exUrls = "\"" + expandedUrl + "\"";
                                    iCheck2 = false;
                                } else {
                                    exUrls = exUrls + ", " + "\"" + expandedUrl + "\"";
                                }
                            }
                        }
                    }

//                    System.out.println(forXima);
                    test(timestamp, username, idStr, hashtagsS, mensionsS, urls, exUrls);
                }
            }
        }

        System.out.println("Test ends!");

    }
}
