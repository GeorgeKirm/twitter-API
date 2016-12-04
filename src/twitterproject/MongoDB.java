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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author x
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
            db = mongo.getDatabase("tweetsDB");
            
            collection = db.getCollection("tweetsCollection");
            sleep(300); // delete me
            System.out.println("Connected to Mongo DB!");
        } catch (MongoException ex) {
            System.out.println("MongoException : " + ex.getMessage());
        } catch (InterruptedException ex) { //delete me
            Logger.getLogger(MongoDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public MongoCollection<Document> collectionGetter(){
        return collection;
    }
    public MongoDatabase dataBaseGetter(){
        return db;
    }
    public Mongo mongoGetter(){ // Not useable for the moment, delete if not needed
        return mongo;
    }
    public void readDataBase() {
        // read from the database and print
        System.out.println("Reading database:");
        
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            int i=0;
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
        System.out.println("Reading database:");
        
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
          if (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
          }
          if (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
          }
          if (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
          }
          if (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
          }
          if (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
          }
        } finally {
          cursor.close();
        }
        System.out.println("Fisnish reading database!");
    }
}
