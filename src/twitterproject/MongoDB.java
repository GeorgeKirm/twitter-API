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
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static java.lang.Thread.sleep;

/**
 * This class creates the 2 collections of the database (one that stores the
 * tweets raw and one that keeps the usefull information from the first), shows
 * their content, drops them or shows the size of them, furthermore it
 * calculates the similarity between the users of the tweets and stores them to
 * files
 *
 * @author x
 */
public class MongoDB {

    private MongoDatabase db;
    private MongoCollection<Document> collectionNo1;
    private MongoCollection<Document> collectionNo2;
    private MongoClient mongo;
    private final List<String> namesOfTheFields; // names of the fields of collectionNo2
    private ArrayDeque<ArrayDeque<Float>> similarityWithFinalResults;
    private ArrayDeque<ArrayDeque<Float>> similarityWithIDstr;
    private ArrayDeque<ArrayDeque<Float>> similarityWithHashtags;
    private ArrayDeque<ArrayDeque<Float>> similarityWithMentions;
    private ArrayDeque<ArrayDeque<Float>> similarityWithURLs;

    /**
     * Initialise an ArrayList (contains the names of the fields that are used
     * in the "collectionNo2"),an object for the database ("mongo") and gets the
     * 2 collections from it ("collectionNo1" which stores all the tweets raw
     * and "collectionNo2" which stores the usefull data of these tweets in a
     * version easy to calculate similarity).
     */
    MongoDB() {
        namesOfTheFields = new ArrayList<>();
        namesOfTheFields.add("Timestamp");
        namesOfTheFields.add("Id_String");
        namesOfTheFields.add("Hashtags");
        namesOfTheFields.add("Mentions");
        namesOfTheFields.add("URLs");
        namesOfTheFields.add("exURLs");
        try {
            //initialize MongoDB, set configuration and load collectionNo1
            System.out.println("Connecting to Mongo DB..");
            mongo = new MongoClient();
//            db = mongo.getDatabase("tweetsDBT");
            db = mongo.getDatabase("tweetsDB");

            collectionNo1 = db.getCollection("tweetsCollection");
            collectionNo2 = db.getCollection("tweetsCollectionTest");
            sleep(300); // delete me
            System.out.println("Connected to Mongo DB!");
        } catch (MongoException ex) {
            System.out.println("MongoException : " + ex.getMessage());
        } catch (InterruptedException ex) { //delete me
            Logger.getLogger(MongoDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Returns the collectionNo1 that stores the information of the tweets raw
     * in json form.
     *
     * @return the big collectionNo1
     */
    public MongoCollection<Document> collectionGetter() {
        return collectionNo1;
    }

    /**
     * Returns the mongoDatabase object that contains the collections with the
     * tweets.
     *
     * @return the database
     */
    public MongoDatabase dataBaseGetter() {
        return db;
    }

    /**
     * Deprecated, will be deleted.
     *
     * @return -
     */
    public Mongo mongoGetter() { // Not useable for the moment, delete if not needed
        return mongo;
    }

    /**
     * Iterates the chosen collection and prints its content.
     *
     * @param whichCollection the collection that will be printed
     * @param limiter how many of the content will be printed
     */
    public void readCollection(int whichCollection, int limiter) {
        // read from the database and print
        System.out.println("Reading collection:");

        MongoCursor<Document> cursor = null;
        switch (whichCollection) {
            case 1:
                if (limiter <= 0) {
                    cursor = collectionNo1.find().iterator();
                } else {
                    cursor = collectionNo1.find().limit(limiter).iterator();
                }
                break;
            case 2:
                if (limiter <= 0) {
                    cursor = collectionNo2.find().iterator();
                } else {
                    cursor = collectionNo2.find().limit(limiter).iterator();
                }
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
        } catch (Exception ex) {
//            System.out.println("Error: " + ex);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        System.out.println("Finish reading collection!");
    }

    /**
     * Closes the database.
     */
    public void closeDataBase() {
        System.out.println("Closing connection to Mongo DB..");
        this.mongo.close();
        System.out.println("Connection to Mongo DB closed!");
    }

    /**
     * Deletes the chosen collection from the database.
     *
     * @param whichCollection the chosen collection to be deleted
     */
    public void dropCollection(int whichCollection) {
        System.out.println("Droping collection..");
        switch (whichCollection) {
            case 1:
                this.collectionNo1.drop();
                break;
            case 2:
                this.collectionNo2.drop();
                break;
            default:
                System.out.println("Error: No such collection! (You need to give a number for the collection like 1 or 2)");
                break;
        }
        System.out.println("Collection droped!");
    }

    /**
     * Prints how many tweets/users the chosen collection contains.
     *
     * @param whichCollection the chosen collection
     */
    public void showTweetsCount(int whichCollection) {
        switch (whichCollection) {
            case 1:
                System.out.println("Number of tweets: " + this.collectionNo1.count());
                break;
            case 2:
                System.out.println("Number of tweets: " + this.collectionNo2.count());
                break;
            default:
                System.out.println("Error: No such collection! (You need to give a number for the collection like 1 or 2)");
                break;
        }

    }

    /**
     * Deprecated will be deleted soon. used for test perpuses for indexing
     */
    public void elegxosIndexing() {
//        BasicDBObject whereQuery = new BasicDBObject();
//        whereQuery.put("Onoma", "ShaneinCurry");
//        collectionNo2.distinct("Hashtags", null);
        
//        ListIndexesIterable indexes = collectionNo2.listIndexes(BasicDBObject.class);
//        MongoCursor cursor3 = indexes.iterator();
//        while (cursor3.hasNext()) {
//            BasicDBObject getCreatedIdx = (BasicDBObject) cursor3.next();
//            System.out.println("index is: " + getCreatedIdx.toString());
//        }
//        System.out.println("Single search!");
//        BasicDBObject whereQuery = new BasicDBObject();9
//        whereQuery.put("Onoma", "PostSexyLips");
//        System.out.println(collectionNo2.find(whereQuery).modifiers(new Document("$explain", true)).first());
    }

    /**
     * Writes the arrays created from similarity() to .csv files to be used in
     * "Gephi"..................................................................
     * UsersTable.csv == contains final similarity..............................
     * retweetsTable.csv == contains the similarity between retweeted status....
     * hashtagsTable.csv == contains the similarity between hashtags............
     * mentionsTable.csv == contains the similarity between mensios.............
     * urlsTable.csv == contains the similarity between urls....................
     *
     * The .csv file is in the form:--------------------------------------------
     * -----------------;Name1 Name2--------------------------------------------
     * -----------------Name1 0.0 0.1-------------------------------------------
     * -----------------Name2 0.0 0.0-------------------------------------------
     */
    public void writeOutCSVs() {
        System.out.println("Writing the arrays to .csv files...");
        try {
            PrintWriter writer = new PrintWriter("UsersTable.csv", "UTF-8");
            PrintWriter writerI = new PrintWriter("retweetsTable.csv", "UTF-8");
            PrintWriter writerH = new PrintWriter("hashtagsTable.csv", "UTF-8");
            PrintWriter writerM = new PrintWriter("mentionsTable.csv", "UTF-8");
            PrintWriter writerU = new PrintWriter("urlsTable.csv", "UTF-8");
            int sizeOfLoula = similarityWithFinalResults.size();
            String containsTheNamesOfTheUsers = ";";
            String lastUser = ""; // last user has all 0.0 as we have his values from the other users
            {
                MongoCursor<Document> cursor = collectionNo2.find().limit(similarityWithFinalResults.size() + 1).iterator();
                Document thisCursor;
                for (int i = 0; i < sizeOfLoula; i++) {
                    thisCursor = cursor.next();
                    containsTheNamesOfTheUsers = containsTheNamesOfTheUsers + (String) (thisCursor.get("Onoma")) + " ";
                    lastUser = lastUser + "0.0 ";
                    if (i % 100 == 0) { // Stores them every 100 so not to have memory problems
                        writer.print(containsTheNamesOfTheUsers);
                        writerI.print(containsTheNamesOfTheUsers);
                        writerH.print(containsTheNamesOfTheUsers);
                        writerM.print(containsTheNamesOfTheUsers);
                        writerU.print(containsTheNamesOfTheUsers);
                        containsTheNamesOfTheUsers = "";
                    }
                }
                thisCursor = cursor.next();
                containsTheNamesOfTheUsers = containsTheNamesOfTheUsers + (String) (thisCursor.get("Onoma")) + " ";
                lastUser = (String) (thisCursor.get("Onoma")) + " " + lastUser;
            }
            writer.println(containsTheNamesOfTheUsers);
            writerI.println(containsTheNamesOfTheUsers);
            writerH.println(containsTheNamesOfTheUsers);
            writerM.println(containsTheNamesOfTheUsers);
            writerU.println(containsTheNamesOfTheUsers);
            MongoCursor<Document> cursor = collectionNo2.find().limit(similarityWithFinalResults.size() + 1).iterator();
            Document thisCursor = cursor.next();
            String userField = (String) (thisCursor.get("Onoma"));
            for (int i = 0; i < sizeOfLoula; i++) {
                ArrayDeque tempFinalList = similarityWithFinalResults.pop();
                ArrayDeque tempIDretweetList = similarityWithIDstr.pop();
                ArrayDeque tempHashtagsList = similarityWithHashtags.pop();
                ArrayDeque tempMentionsList = similarityWithMentions.pop();
                ArrayDeque tempURLlist = similarityWithURLs.pop();
                writer.print(userField + " "); // first goes the name
                writerI.print(userField + " ");
                writerH.print(userField + " ");
                writerM.print(userField + " ");
                writerU.print(userField + " ");
                thisCursor = cursor.next();
                userField = (String) (thisCursor.get("Onoma"));

                // We have stored in the arrays only one of the triangles as we dont need redirected edges
                // and we already got the values for the users once. So we fill the other half with zeros
                int sizeOfVoula = tempFinalList.size();
                for (int j = 0; j < (sizeOfLoula + 1) - sizeOfVoula; j++) {
                    writer.print("0.0 ");
                    writerI.print("0.0 ");
                    writerH.print("0.0 ");
                    writerM.print("0.0 ");
                    writerU.print("0.0 ");
                }
                // Starts writing the accual calculated values
                for (int j = 0; j < sizeOfVoula; j++) {
                    writer.print(tempFinalList.pop() + " ");
                    writerI.print(tempIDretweetList.pop() + " ");
                    writerH.print(tempHashtagsList.pop() + " ");
                    writerM.print(tempMentionsList.pop() + " ");
                    writerU.print(tempURLlist.pop() + " ");
                }
                writer.println();
                writerI.println();
                writerH.println();
                writerM.println();
                writerU.println();
            }
            // write the final user
            writer.print(lastUser);
            writerI.print(lastUser);
            writerH.print(lastUser);
            writerM.print(lastUser);
            writerU.print(lastUser);
            writer.close();
            writerI.close();
            writerH.close();
            writerM.close();
            writerU.close();
        } catch (IOException e) {
            System.out.println("Error! files");
            System.out.println(e);
        }
        System.out.println("Done!");
    }

    /**
     * Implements the cosine similarity between the 2 given arraylists. It
     * returns 0.01 if both arraylists are empty, 0 if one of them only is empty
     * or else the value generated from cosine similarity.
     *
     * @param field1 the first arraylist for the cosine similarity
     * @param field2 the first arraylist for the cosine similarity
     * @return the cosine similarity between the 2 arraylists
     */
    private float cosineSimilarity(ArrayList field1, ArrayList field2) {
        if (field1.isEmpty() && field2.isEmpty()) {
            return (float) 0.01;
        } else if (field1.isEmpty() || field2.isEmpty()) {
            return 0;
        } else {
            HashMap<String, Integer> arrayOfPotition = new HashMap<>();
            ArrayDeque<Boolean> pinakas1 = new ArrayDeque<>();
            ArrayList<Boolean> pinakas2 = new ArrayList<>();
            for (int i = 0; i < field1.size(); i++) {
                Integer ii = arrayOfPotition.putIfAbsent(field1.get(i).toString(), arrayOfPotition.size());
                if (ii == null) { // den uparxei mesa
                    pinakas1.add(true);
                    pinakas2.add(false);
                }
//                else {
//                    pinakas1.add(false);
//                }
            }
            for (int i = 0; i < field2.size(); i++) {
                Integer ii = arrayOfPotition.putIfAbsent(field2.get(i).toString(), arrayOfPotition.size());
                if (ii == null) { //den upirxe mesa
                    pinakas1.add(false);
                    pinakas2.add(true);
                } else {
                    pinakas2.set(ii, true);
                }
            }
            /*/ omoiotita
            System.out.println("ArrayList me tis times tou 1ou user: " + field1);
            System.out.println("ArrayList me tis times tou 2ou user: " + field2);
            System.out.println("Pinakas gia to similarity tou 1ou user: " + pinakas1);
            System.out.println("Pinakas gia to similarity tou 2ou user: " + pinakas2);
            System.out.println("To Hashmap pou exei ta kleidia: " + arrayOfPotition);
            // */
            int athrisma = 0;
            int metro1 = 0;
            int metro2 = 0;
            for (int i = 0; i < pinakas2.size(); i++) {
                int loula1 = pinakas1.pop() ? 1 : 0;
                int loula2 = pinakas2.get(i) ? 1 : 0;
                athrisma = athrisma + (loula1 * loula2);
                metro1 = metro1 + (loula1 * loula1);
                metro2 = metro2 + (loula2 * loula2);
            }
            double metro0 = (Math.sqrt(metro1) * Math.sqrt(metro2));
            return (float) (athrisma / metro0);
        }
    }

    /**
     * Iterates the "collectionNo2" to calculate the cosine similarity and
     * stores the results on 5 arrayDeque (for retweeten status, hashtags,
     * mentions, urls and for all of them), in the end it calls "writeOutCSVs()"
     * to export the arrays. In the final arrayDeque it stores the 4 other
     * arrayDeque added together normalized like follows: retweeten status * 0.1
     * hashtags * 0.3 mentions * 0.3 urls * 0.3
     */
    public void cosineSimilarityIterate() {
        System.out.println("Calculating cosine similarity...");
        /*/ time
        long tDelta1 = 0;
        long tDelta2 = 0;
        long tStart1, tStart2, tEnd1, tEnd2;
        // */
        similarityWithFinalResults = new ArrayDeque<>((int) collectionNo2.count());
        similarityWithIDstr = new ArrayDeque<>((int) collectionNo2.count());
        similarityWithHashtags = new ArrayDeque<>((int) collectionNo2.count());
        similarityWithMentions = new ArrayDeque<>((int) collectionNo2.count());
        similarityWithURLs = new ArrayDeque<>((int) collectionNo2.count());
        ArrayDeque<Float> pinakasTemp;
        ArrayDeque<Float> pinakasTempI;
        ArrayDeque<Float> pinakasTempH;
        ArrayDeque<Float> pinakasTempM;
        ArrayDeque<Float> pinakasTempU;
        int ii = 0;
//        for (Document thisCursor1 : collectionNo2.find().limit(limitToSearch)) {
        for (Document thisCursor1 : collectionNo2.find()) {
            /*/ omoiotita
            System.out.println("==User No" + ii);
            // */
            ii++;
            pinakasTemp = new ArrayDeque<>();
            pinakasTempI = new ArrayDeque<>();
            pinakasTempH = new ArrayDeque<>();
            pinakasTempM = new ArrayDeque<>();
            pinakasTempU = new ArrayDeque<>();
            /*/ time
            tStart1 = System.currentTimeMillis();
            // */
            ArrayList field11 = (ArrayList) thisCursor1.get(namesOfTheFields.get(1));
            ArrayList field12 = (ArrayList) thisCursor1.get(namesOfTheFields.get(2));
            ArrayList field13 = (ArrayList) thisCursor1.get(namesOfTheFields.get(3));
            ArrayList field14 = (ArrayList) thisCursor1.get(namesOfTheFields.get(4));
            ArrayList field15 = (ArrayList) thisCursor1.get(namesOfTheFields.get(5));
            /*/ time
            tEnd1 = System.currentTimeMillis();
            tDelta1 = tEnd1 - tStart1 + tDelta1;
            // */
//            for (Document thisCursor2 : collectionNo2.find().skip(ii).limit(limitToSearch)) {
//            for (Document thisCursor2 : collectionNo2.find().limit(limitToSearch)) {
            for (Document thisCursor2 : collectionNo2.find().skip(ii)) {
                /*/ time
                tStart1 = System.currentTimeMillis();
                // */
                ArrayList field21 = (ArrayList) thisCursor2.get(namesOfTheFields.get(1));
                ArrayList field22 = (ArrayList) thisCursor2.get(namesOfTheFields.get(2));
                ArrayList field23 = (ArrayList) thisCursor2.get(namesOfTheFields.get(3));
                ArrayList field24 = (ArrayList) thisCursor2.get(namesOfTheFields.get(4));
                ArrayList field25 = (ArrayList) thisCursor2.get(namesOfTheFields.get(5));
                /*/ time
                    tEnd1 = System.currentTimeMillis();
                    tDelta1 = tEnd1 - tStart1 + tDelta1;
                    tStart2 = System.currentTimeMillis();
                    // */
 /*/ omoiotita
                    System.out.println("  1. idStringsHashtags ===");
                    // */
                DecimalFormat df = new DecimalFormat("0.#####");
                float tvos1 = cosineSimilarity(field11, field21); // tempValueOfSimilarity1
                tvos1 = Float.parseFloat(df.format(tvos1));
                pinakasTempI.add(tvos1);
                tvos1 = (float) (tvos1 * 0.1);
                /*/ omoiotita
                    System.out.println("  2. Hashtags ===");
                // */
                float tvos2 = cosineSimilarity(field12, field22); // tempValueOfSimilarity2
                tvos2 = Float.parseFloat(df.format(tvos2));
                pinakasTempH.add(tvos2);
                tvos1 = (float) (tvos1 + tvos2 * 0.3);
                /*/ omoiotita
                    System.out.println("  3. Mensions ===");
                // */
                tvos2 = cosineSimilarity(field13, field23);
                tvos2 = Float.parseFloat(df.format(tvos2));
                pinakasTempM.add(tvos2);
                tvos1 = (float) (tvos1 + tvos2 * 0.3);
                /*/ omoiotita
                    System.out.println("  4. Urls1 ===");
                // */
                tvos2 = cosineSimilarity(field14, field24);
                /*/ omoiotita
                    System.out.println("  5. Urls2 ===");
                // */
                float tvos3 = cosineSimilarity(field15, field25);  // tempValueOfSimilarity3
                tvos3 = Float.parseFloat(df.format(tvos3));
                pinakasTempU.add(((tvos2 + tvos3) / 2));
                tvos1 = (float) ((tvos1) + (tvos2 * 0.15) + (tvos3 * 0.15));
                tvos1 = Float.parseFloat(df.format(tvos1));
                pinakasTemp.add(tvos1);
                /*/ time
                    tEnd2 = System.currentTimeMillis();
                    tDelta2 = tEnd2 - tStart2 + tDelta2;
                // */
            }
            if (!pinakasTemp.isEmpty()) {
                similarityWithFinalResults.add(pinakasTemp);
                similarityWithIDstr.add(pinakasTempI);
                similarityWithHashtags.add(pinakasTempH);
                similarityWithMentions.add(pinakasTempM);
                similarityWithURLs.add(pinakasTempU);
            }
        }
        System.out.println("Users: " + similarityWithFinalResults.size()+1);
        System.out.println("Done!");
//        System.out.println("Sunolikos: " + similarityWithFinalResults);
//        System.out.println("Retweeted: " + similarityWithIDstr);
//        System.out.println("Hashtagss: " + similarityWithHashtags);
//        System.out.println("Mensionss: " + similarityWithMentions);
//        System.out.println("Urlssssss: " + similarityWithURLs);
        writeOutCSVs();
        /*/ time
        {
            double elapsedSeconDs;
            int elapsedSeconds;
            int elapsedMinutes = 0;
            int elapsedHours = 0;
            elapsedSeconDs = tDelta1 / 1000.0;
            elapsedSeconds = (int) tDelta1 / 1000;
            if (elapsedSeconds >= 60) {
                elapsedMinutes = elapsedSeconds / 60;
                elapsedSeconds = elapsedSeconds % 60;
            }
            if (elapsedMinutes >= 60) {
                elapsedHours = elapsedMinutes / 60;
                elapsedMinutes = elapsedMinutes % 60;
            }
            System.out.println("Time elapased: " + elapsedHours + ":" + elapsedMinutes + ":" + elapsedSeconds);
            System.out.println(elapsedSeconDs + "s");
        }
        {
            double elapsedSeconDs;
            int elapsedSeconds;
            int elapsedMinutes = 0;
            int elapsedHours = 0;
            elapsedSeconDs = tDelta2 / 1000.0;
            elapsedSeconds = (int) tDelta2 / 1000;
            if (elapsedSeconds >= 60) {
                elapsedMinutes = elapsedSeconds / 60;
                elapsedSeconds = elapsedSeconds % 60;
            }
            if (elapsedMinutes >= 60) {
                elapsedHours = elapsedMinutes / 60;
                elapsedMinutes = elapsedMinutes % 60;
            }
            System.out.println("Time elapsed: " + elapsedHours + ":" + elapsedMinutes + ":" + elapsedSeconds);
            System.out.println(elapsedSeconDs + "s");
        }
        // */
    }

    /**
     * Stores the values of a field of a tweet to a user by extending his values
     * as the user has already stored in the collectionNo1, tweets from same
     * user are saved in same line.
     *
     * @param pedio the field that the values belong at
     * @param username the username of the user that made the tweet
     * @param loula the values of the field of the tweet of that user
     */
    private void getUsefullInfoFromColNo1(String pedio, String username, String loula) {
        loula = loula.replaceAll("\"", "");
        if (loula.equals("")) {
        } else {
            List<String> voulaTimestamp = Arrays.asList(loula.split("\\s*,\\s*"));
            collectionNo2.updateOne(new Document("Onoma", username),
                    new Document("$pushAll", new Document(pedio, voulaTimestamp)));
        }
    }

    /**
     * Stores the usefull information taken a tweet from the iteration to the
     * new collectionNo1, all the tweets of the same user are stored in the same
     * line.
     *
     * @param timestamp timestamp of that tweet
     * @param username the username of the user that did the tweet
     * @param idStr the id of the retweet of that tweet if it exist
     * @param hashtagsS the hashtags of that tweet
     * @param mensionsS the mentions of that tweet
     * @param urls the urls of that tweet
     * @param exUrls the extended version of the urls
     */
    private void getUsefullInfoFromColNo1(String timestamp, String username, String idStr, String hashtagsS, String mensionsS, String urls, String exUrls) {
//        if (checkerFirst) {
//            BasicDBObject index = new BasicDBObject("Onoma", 1);
//            collectionNo2.createIndex(index);
//            checkerFirst = false;
//        }
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("Onoma", username);
        MongoCursor<Document> cursor = collectionNo2.find(whereQuery).iterator();
        if (cursor.hasNext()) {
            getUsefullInfoFromColNo1(namesOfTheFields.get(0), username, timestamp);
            getUsefullInfoFromColNo1(namesOfTheFields.get(1), username, idStr);
            getUsefullInfoFromColNo1(namesOfTheFields.get(2), username, hashtagsS);
            getUsefullInfoFromColNo1(namesOfTheFields.get(3), username, mensionsS);
            getUsefullInfoFromColNo1(namesOfTheFields.get(4), username, urls);
            getUsefullInfoFromColNo1(namesOfTheFields.get(5), username, exUrls);
        } else {
            String newLineFor2ndCollection = "{";
            newLineFor2ndCollection = newLineFor2ndCollection + "\nTimestamp: [" + timestamp + "]";
            newLineFor2ndCollection = newLineFor2ndCollection + "\nOnoma: \"" + username + "\"";
            if (idStr == null) {
                newLineFor2ndCollection = newLineFor2ndCollection + "\nId_String: []";
            } else {
                newLineFor2ndCollection = newLineFor2ndCollection + "\nId_String: [" + idStr + "]";
            }
            newLineFor2ndCollection = newLineFor2ndCollection + "\nHashtags: [" + hashtagsS + "]";
            newLineFor2ndCollection = newLineFor2ndCollection + "\nMentions: [" + mensionsS + "]";
            newLineFor2ndCollection = newLineFor2ndCollection + "\nURLs: [" + urls + "]" + "\nexURLs: [" + exUrls + "]";
            newLineFor2ndCollection = newLineFor2ndCollection + "\n}";
            Document doc = Document.parse(newLineFor2ndCollection);
            collectionNo2.insertOne(doc);
        }

    }

    /**
     * Iterates the first "limitOfTweets" tweets of collectionNo1 No1
     * ("collectionNo1") to get its usefull information and calls another
     * function for them to be stored in collectionNo1 No2 ("collectionNo2").
     *
     * @param limitOfTweets the new collectionNo1 will be created with this
     * limit of tweets
     */
    public void getUsefullInfoFromColNo1(int limitOfTweets) {
        // read from the database and print
        System.out.println("Test starts:");
        dropCollection(2);
        BasicDBObject index = new BasicDBObject("Onoma", 1);
        collectionNo2.createIndex(index);
        try (MongoCursor<Document> cursor = collectionNo1.find().iterator()) {
            int coula = 0;
            while (cursor.hasNext() && coula < limitOfTweets) {
//            while (cursor.hasNext() && i <= 1000) {
//                System.out.println(i);
                Document thisCursor = cursor.next();
                //System.out.println(thisCursor); //prints full json format

                String text = thisCursor.get("text").toString(); //text of the tweet

                if (!text.contains("…")) { //if the text has ellipsis, ignore it
//                    System.out.println("keimeno ok:");
                    String timestamp = thisCursor.get("timestamp_ms").toString();
                    timestamp = "\"" + timestamp + "\"";
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
                    getUsefullInfoFromColNo1(timestamp, username, idStr, hashtagsS, mensionsS, urls, exUrls);
                    coula++;
                }
            }
        }
        System.out.println("Test ends!");
    }
}
