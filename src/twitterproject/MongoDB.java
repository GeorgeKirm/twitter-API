/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 *
 * @author x
 */
public class MongoDB {
    
    private DB db;
    private DBCollection collection;
    private MongoClient mongo;
    
    MongoDB() {
        try {
            //initialize MongoDB, set configuration and load collection
            System.out.println("Connecting to Mongo DB..");
            mongo = new MongoClient();
            db = mongo.getDB("tweetsDB");
            collection = db.getCollection("tweetsCollection");
            System.out.println("Connected to Mongo DB!");
        } catch (MongoException ex) {
            System.out.println("MongoException : " + ex.getMessage());
        }
    }
    public DBCollection collectionGetter(){
        return collection;
    }
    public DB dataBaseGetter(){
        return db;
    }
    public Mongo mongoGetter(){ // Not useable for the moment, delete if not needed
        return mongo;
    }
    public void readDataBase() {
        // read from the database and print
        System.out.println("Reading database:");
        DBCursor cursorDoc;
        cursorDoc = this.collectionGetter().find();

        int i=0;
        while (cursorDoc.hasNext()) {
            i++;
            System.out.println(i);
            System.out.println(cursorDoc.next());
        } 
        System.out.println("Fisnish reading database!");
    }
    public void closeDataBase() {
        System.out.println("Closing connection to Mongo DB..");
        this.mongo.close();
        System.out.println("Connection to Mongo DB closed!");
    }
    public void dropCollection() {
        System.out.println("Droping database..");
        this.collection.drop();
        System.out.println("Database droped!");
    }
}
