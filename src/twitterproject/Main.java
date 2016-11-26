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
        do {
            int a;
            Scanner in = new Scanner(System.in);
            System.out.println("\n");
            System.out.println("== Choose what you want to do ==");
            System.out.println("1. Get new tweets and store them to database");
            System.out.println("2. Drop old database");
            System.out.println("3. Read database");
            System.out.println("4. Exit");
            System.out.print("(press int): ");
            a = in.nextInt();
            MongoDB dataBase;
            Twitter loula;
            switch (a){
                case 1:
                    dataBase = new MongoDB();
                    loula = new Twitter(180);
                    loula.listener(dataBase);
                    dataBase.closeDataBase();
                    break;
                case 2:
                    dataBase = new MongoDB();
                    dataBase.dropCollection();
                    dataBase.closeDataBase();
                    break;
                case 3:
                    dataBase = new MongoDB();
                    dataBase.readDataBase();
                    dataBase.closeDataBase();
                    break;
                default:
                    pass = true;
            }
        } while (pass == false);
    }
}
