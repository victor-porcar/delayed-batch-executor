package com.github.victormpcmun.delayedbatchexecutor.simulator;

public class SimulatorUtils {

    static void sleepCurrentThread(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException("InterruptedException", e);
        }
    }


    static String concatenateInt(long long1, int int2) {
        return long1 + "_" + int2;
    }
}
