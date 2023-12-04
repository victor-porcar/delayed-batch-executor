package com.github.victormpcmun.delayedbatchexecutor.simulator;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class ClientParallelRequestSimulator extends TimerTask {

    private final Logger log = LoggerFactory.getLogger(getClass());

    int currentSecond;

    int requestsPerSecond;

    BiFunction<Long,Integer,String> requestFunction;

    int testDurationInSeconds;
    int totalRequestsSinceBeginning;

    ExecutorService executorService;

    Set<String> allRequestsSet;
    AtomicLong maxTimeInFinishingCall;

    int totalExpectedRequests;

    public ClientParallelRequestSimulator(
            int requestsPerSecond,
            int testDurationInSecond,
            BiFunction<Long,Integer,String> requestFunction) {

        this.totalRequestsSinceBeginning =0;
        this.currentSecond =0;
        this.requestsPerSecond = requestsPerSecond;
        this.requestFunction = requestFunction;

        this.testDurationInSeconds=testDurationInSecond;
        this.executorService = Executors.newFixedThreadPool(10000);
        this.allRequestsSet = Collections.synchronizedSet(new HashSet<>());
        this.maxTimeInFinishingCall= new AtomicLong(-1);
        this.totalExpectedRequests = requestsPerSecond * testDurationInSeconds;
    }


    public void run() {

        if (currentSecond > (testDurationInSeconds-1)) {
            // do nothing
            return;
        }

        List<Future<Void>> threads = new ArrayList<>();
        int threadCounter;
        for (threadCounter = 0; threadCounter < requestsPerSecond; threadCounter++) {
            Callable<Void> callable = getCallableForThread(
                    threadCounter,
                    this.requestFunction,
                    this);
            threads.add(executorService.submit(callable));
        }

        log.info("begin second {} => created {} requests", currentSecond, threadCounter);

        currentSecond++;

    }

    public void go() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(this, 0, 1000);
    }


    public Set<String> getCallsNotFinished() {
        return allRequestsSet;
    }


    private Callable<Void> getCallableForThread(
            int threadNumber,
            BiFunction<Long,Integer,String> biFunction,
            ClientParallelRequestSimulator clientParallelRequestSimulator) {
        return () -> {

            int delay = getRandomIntegerFromInterval(0, 999);
            SimulatorUtils.sleepCurrentThread(delay);

            long initTimestamp = System.currentTimeMillis();

            String callIdentifier = SimulatorUtils.concatenateInt(initTimestamp,threadNumber);

            clientParallelRequestSimulator.allRequestsSet.add(callIdentifier);
            clientParallelRequestSimulator.totalRequestsSinceBeginning++;

            String result = biFunction.apply(initTimestamp, threadNumber);


            clientParallelRequestSimulator.allRequestsSet.remove(callIdentifier);
            long callDuration = System.currentTimeMillis() - initTimestamp;


            if (clientParallelRequestSimulator.maxTimeInFinishingCall.longValue()<callDuration) {
                clientParallelRequestSimulator.maxTimeInFinishingCall.set(callDuration);
            }


            if (!result.equals(callIdentifier)) {
                throw new RuntimeException("Unexpected value blabla");
            }
            return null;
        };
    }


    private Integer getRandomIntegerFromInterval(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public void logExecutionReport() {
        log.info("---------------------------------------------------");

        log.info("total expected requests: {}" , getTotalExpectedRequests());
        log.info("total total Requests Since Beginning: {}" , getTotalRequestsSinceBeginning());
        log.info("Calls not finished: {}" , getCallsNotFinished());
        log.info("Max call duration in time {}", maxTimeInFinishingCall);
    }


    public int getTotalRequestsSinceBeginning() {
        return totalRequestsSinceBeginning;
    }


    public int getTotalExpectedRequests() {
        return totalExpectedRequests;
    }

}
