package com.vp;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.vp.delayedbatchexecutor.*;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DelayedBatchExecutorTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final int DO_QUERY_SIMULATION_MIN_TIME = 10;
    private static final int DO_QUERY_SIMULATION_MAX_TIME = 100;

    private static final int SECONDS_SIMULATION = 1;
    private static final int ONE_SECOND_MILLISECONDS = 1000;

    private static final int BYTES_IN_MEGABYTES = 1020*1024;


    private final DelayedBatchExecutor3<String, String, String> delayedExecutorMinBoundary = DelayedBatchExecutor.define( Duration.ofMillis(1), 1,this::doQueryMultipleParametersSimulation);
    private final DelayedBatchExecutor3<String, String, String> delayedExecutorMaxBoundary = DelayedBatchExecutor.define(DelayedBatchExecutor.MAX_TIME, DelayedBatchExecutor.MAX_SIZE, this::doQueryMultipleParametersSimulation);


    @Test
    public void testMinBoundaries() {
        createThreadsAndExecute(2, delayedExecutorMinBoundary);
        createThreadsAndExecute(64, delayedExecutorMinBoundary);
        createThreadsAndExecute(128, delayedExecutorMinBoundary);

    }

    @Test
    public void testMaxBoundaries() {
        createThreadsAndExecute(1, delayedExecutorMaxBoundary);
        createThreadsAndExecute(64, delayedExecutorMaxBoundary);
        createThreadsAndExecute(128, delayedExecutorMaxBoundary);
    }


    private void createThreadsAndExecute(int threadsPerSecond, DelayedBatchExecutor3<String, String, String> delayedBulkExecutor) {
        log.info("======>  BeginningTest for DelayedBulkExecutor: {} => Threads Per Second: {}",   delayedBulkExecutor, threadsPerSecond);
        List<Thread> allCreatedThreads = new ArrayList<>();

        for (int second = 1; second <= SECONDS_SIMULATION; second++) {
            String threadsPrefixName = "THREAD_SECOND" + second;
            List<Thread> threads = createThreadsWithPrefixName(threadsPerSecond, threadsPrefixName, delayedBulkExecutor);
            threads.forEach(Thread::start);
            pause(ONE_SECOND_MILLISECONDS);
            allCreatedThreads.addAll(threads);
        }

        allCreatedThreads.forEach(this::join); // wait until all threads have finished
        log.info("ENVIRONMENT INFO: {}", getEnvironmentInfo());

       }

    private List<Thread> createThreadsWithPrefixName(int totalThreads, String prefixName, DelayedBatchExecutor3<String, String, String> delayedBulkExecutor) {
        List<Thread> threads = new ArrayList<>();
        for (int threadCounter = 0; threadCounter < totalThreads; threadCounter++) {
            final String threadName=prefixName + threadCounter;
            Thread thread = new Thread(() -> {
                String arg1 = threadName + "_ARG1";
                String arg2 = threadName + "_ARG2";
                String expectedResult = simulateQueryResultForArguments(arg1, arg2);

                // FIRST TIME SYNC
                String result = delayedBulkExecutor.execute(arg1, arg2);
                Assert.assertEquals(result, expectedResult);

                // SECOND TIME SYNC
                result = delayedBulkExecutor.execute(arg1, arg2);
                Assert.assertEquals(result, expectedResult);

                // ASYNC
                Future<String>  futureResult = delayedBulkExecutor.executeAsFuture(arg1, arg2);
                // do something
                randomPause(100, 500);
                try {
                    Assert.assertEquals(futureResult.get(), expectedResult);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Future problem. it shouldn't happen ever", e);
                }

            });
            thread.setName(threadName);
            threads.add(thread);
        }
        return threads;
    }


    private List<String> doQueryMultipleParametersSimulation(List<String> listArg1, List<String> listArg2) {

        List<String> resultList = new ArrayList<>();

        for (int i=0; i<listArg1.size(); i++) {
            resultList.add(simulateQueryResultForArguments(listArg1.get(i), listArg2.get(i)));
        }

        randomPause(DO_QUERY_SIMULATION_MIN_TIME, DO_QUERY_SIMULATION_MAX_TIME);
        return resultList;
    }




    private void randomPause(int millisecondsInit, int millisecondsEnd) {
        try {
            Thread.sleep(millisecondsInit + (int) (Math.random() * millisecondsEnd));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void pause(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void join(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String simulateQueryResultForArguments(String a, String b) {
        return String.format("%s SIMULATION %s", a,b);
    }

    private String   getEnvironmentInfo() {
        Runtime rt = Runtime.getRuntime();
        int totalThreads = Thread.getAllStackTraces().keySet().size();
        long usedMemoryMB = (rt.totalMemory() - rt.freeMemory()) / BYTES_IN_MEGABYTES;
        long freeMemoryMB = rt.freeMemory() / BYTES_IN_MEGABYTES;
        long totalMemoryMB = rt.totalMemory() / BYTES_IN_MEGABYTES;
        long maxMemoryMB = rt.maxMemory() / BYTES_IN_MEGABYTES;
        return String.format("Total Threads:%d,  usedMemoryMB:%d, freeMemoryMB:%d, totalMemoryMB:%d, maxMemoryMB:%d", totalThreads, usedMemoryMB, freeMemoryMB, totalMemoryMB, maxMemoryMB);
    }

}
