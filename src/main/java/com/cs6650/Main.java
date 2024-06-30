package com.cs6650;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Rebecca Zhang
 * Created on 2024-06-26
 */
public class Main {

    public static void main(String[] args) throws Exception {

        ConsumerManager manager = new ConsumerManager();
        manager.startConsumers();

        System.out.println("Press 'q' or 'Q' to shutdown.");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String input = reader.readLine();
            if (input != null && input.equalsIgnoreCase("q")) {
                break;
            }
        }

        manager.shutdown();
    }

}
