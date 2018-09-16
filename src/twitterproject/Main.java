/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import java.util.Scanner;

/**
 * Main class of the project, creates object to get the tweets from the stream
 * API and the object to process the database, it also has a menu for the user
 * of the program.
 * 
 * @author x
 */
public class Main {
    public static void main(String[] args) {
        boolean pass = false;
        Twitter loula;
        MongoDB dataBase = new MongoDB();
        do {
            try {
                int a;
                Scanner in = new Scanner(System.in);
                System.out.println("\n");
                System.out.println("== Choose what you want to do ==");
                System.out.println("1. Download new tweets and store them to collection No1");
                System.out.println("2. Drop collection");
                System.out.println("3. Read collection");
                System.out.println("4. Show how many tweets exist");
                System.out.println("5. Create collection No2 with the usefull info from No1");
                System.out.println("6. Calculate the similarity between the users from collection No2");
                System.out.println("9. Exit");
                System.out.print("(press int): ");
                a = in.nextInt();
                System.out.println("===================");
                long tStart = System.currentTimeMillis();
                long tEnd, tDelta;
                double elapsedSeconDs;
                int elapsedSeconds;
                int elapsedMinutes = 0;
                int elapsedHours = 0;
                switch (a) {
                    case 1: // new tweets
                        System.out.println("How much time (seconds) you want to download?");
                        System.out.println("(the new tweets will be stored in No1 collection)");
                        System.out.print("(give int): ");
                        a = in.nextInt();
                        loula = new Twitter(a);
                        loula.listener(dataBase);
                        break;
                    case 2: // Drop
                        System.out.println("What collection you want do delete?");
                        System.out.println("(1 is the raw collection and 2 is the edited one)");
                        System.out.print("(give 1 or 2): ");
                        a = in.nextInt();
                        System.out.println("Are you sure? The data will be permenatly lost!");
                        System.out.print("(give 1 for yes and 2 for no): ");
                        int bb = in.nextInt();
                        if (bb == 1) {
                            dataBase.dropCollection(a);
                        } else {
                            System.out.println("Process canceled!");
                        }
                        break;
                    case 3: // sout
                        System.out.println("What collection you want to read?");
                        System.out.println("(1 is the raw collection and 2 is the edited one)");
                        System.out.print("(press int): ");
                        a = in.nextInt();
                        System.out.println("How many tweets you want to read?");
                        System.out.println("(Give 0 for all collection or number for the limit): ");
                        System.out.print("(press int): ");
                        int b = in.nextInt();
                        dataBase.readCollection(a, b);
                        break;
                    case 4: // size
                        System.out.println("From what collection you want to see the size?");
                        System.out.println("(1 is the raw collection and 2 is the edited one)");
                        System.out.print("(press int): ");
                        a = in.nextInt();
                        dataBase.showTweetsCount(a);
                        break;
                    case 5: // new base
                        System.out.println("How many tweets you want to be stored in the No2 collection?: ");
                        System.out.print("(press int): ");
                        a = in.nextInt();
                        dataBase.getUsefullInfoFromColNo1(a);
//                        dataBase.elegxosIndexing();
                        break;
                    case 6: // Similarity
                        dataBase.cosineSimilarityIterate();
                        break;
                    default:
                        pass = true;
                }
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                elapsedSeconDs = tDelta / 1000.0;
                elapsedSeconds = (int) tDelta / 1000;
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
            } catch (Exception ex) {
                System.out.println("\nError: Something went wrong!");
                System.out.println("ex: " + ex);
            }
        } while (pass == false);
        dataBase.closeDataBase();
    }
}