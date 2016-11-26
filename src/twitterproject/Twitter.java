/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import static java.lang.Thread.sleep;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import static java.lang.Thread.sleep;
import static java.lang.Thread.sleep;
import static java.lang.Thread.sleep;

/**
 *
 * @author x
 */
public class Twitter {
    private static ConfigurationBuilder cb;
    private final int tweetCount; // to stop the twitterStream
    Twitter(int tweetCount) {
        this.tweetCount = tweetCount;
        cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("BqEnvpMF3M3vhEg0EVqd7jOMz");
        cb.setOAuthConsumerSecret("sXY8Xrocjs7hWDOJi9tQdaQPHsQ6M6M2LA4Sx0IdNh3Jtf4ue2");
        cb.setOAuthAccessToken("3331853573-6QQCtpjyGRq1H4MQmqbtQc0zq0uVILfTyGDhUTF");
        cb.setOAuthAccessTokenSecret("57ruwsXfBUh8W7BEvF4le5Stjz1QgPMWZ6EfrPjg0AOfa");
        cb.setJSONStoreEnabled(true);
    }
    public void listener(MongoDB dataBase) {
        TwitterStreamFactory tsf = new TwitterStreamFactory(cb.build());
        TwitterStream twitterStream = tsf.getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
//                System.out.println("@" + status.getUser().getScreenName() + " : " + status.getText() + status.getCreatedAt());
                String tweet = TwitterObjectFactory.getRawJSON(status);
//                System.out.println(tweet); //show tweet in json format
                DBObject doc = (DBObject)JSON.parse(tweet);
                dataBase.collectionGetter().insert(doc); //insert in MongoDB
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onException(Exception ex) {
                System.out.println("onException:" + ex.getMessage());
                ex.printStackTrace();
            }

            @Override
            public void onStallWarning(StallWarning sw) {
                System.out.println("Got stall warning:" + sw);
            }
        };
        twitterStream.addListener(listener);        
        FilterQuery fq = new FilterQuery();
        String keywords[] = {"#trump", "#MakeAmericaGreatAgain","#USelection","#NeverHillary","#3rdparty"};

        fq.track(keywords);
        twitterStream.filter(fq);  
        
        try {
            // 1000 = 1 second
            sleep(tweetCount*1000);
        } catch (InterruptedException ex) {
            Logger.getLogger(Twitter.class.getName()).log(Level.SEVERE, null, ex);
        }
        
//        // to stop the stream
//        while (dataBase.collectionGetter().count() < tweetCount) {
//
//        }
        twitterStream.shutdown();
        System.out.println("Closing connection");

    }
}