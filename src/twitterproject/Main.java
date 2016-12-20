/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import java.util.Scanner;

/**
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
                System.out.println("1. Get new tweets and store them to collection No1");
                System.out.println("2. Drop old collection");
                System.out.println("3. Read collection");
                System.out.println("4. Show how many tweets exist");
                System.out.println("5. Test");
                System.out.println("9. Exit");
                System.out.print("(press int): ");
                a = in.nextInt();
                switch (a){
                    case 1:
                        loula = new Twitter(10);
                        loula.listener(dataBase);
                        break;
                    case 2:
                        System.out.print("(press int for collection): ");
                        a = in.nextInt();
                        dataBase.dropCollection(a);
                        break;
                    case 3:
                        System.out.print("(press int for collection): ");
                        a = in.nextInt();
                        dataBase.readCollection(a);
                        break;
                    case 4:
                        dataBase.showTweetsCount();
                        break;
                    case 5:
                        dataBase.test();
                        break;
                    default:
                        pass = true;
                }
            } catch (Exception ex) {
                System.out.print("\nError: Something went wrong! (maybe you didnt gave number)");
            }
        } while (pass == false);
        dataBase.closeDataBase();
    }
}
