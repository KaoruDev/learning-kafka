package com.kaoruk;

import java.util.concurrent.TimeUnit;

/**
 * Created by kaoru on 1/6/17.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("24 Days to days");
        System.out.println(TimeUnit.DAYS.toDays(24));

        System.out.println("24 Days to hours");
        System.out.println(TimeUnit.DAYS.toHours(24));
    }
}
