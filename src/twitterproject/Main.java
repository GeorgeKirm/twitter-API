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
            int a;
            Scanner in = new Scanner(System.in);
            System.out.println("\n");
            System.out.println("== Choose what you want to do ==");
            System.out.println("1. Get new tweets and store them to database");
            System.out.println("2. Drop old database");
            System.out.println("3. Read database");
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
                    dataBase.dropCollection();
                    break;
                case 3:
                    dataBase.readDataBase();
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
        } while (pass == false);
        dataBase.closeDataBase();
    }
}
