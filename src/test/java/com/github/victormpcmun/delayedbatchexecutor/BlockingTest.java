package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.sample.Product;
import com.github.victormpcmun.delayedbatchexecutor.sample.ProductDAO;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class BlockingTest {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final static int THREADS=10;
    private final static String PREFIX = "P";
    Duration duration = Duration.ofMillis(50);
    int maxSize=2;
    private static boolean VERBOSE=true;
    private final DelayedBatchExecutor2<String, Integer> dbe2 = DelayedBatchExecutor2.define(duration, maxSize, this::dbeCallback);

    @Test
    public void blockingNoExceptionTest() {

        Runnable threadTask = () -> {

            Integer randomInteger = getRandomIntegerFromInterval(1,1000);
            String expectedValue =  PREFIX + randomInteger;

            if (VERBOSE) {
                log.info("Before invoking execute with arg {}", randomInteger);
            }

            String result = dbe2.execute(randomInteger); // it will block until the result is available

            if (VERBOSE) {
                log.info("After invoking execute. Expected returned Value {}. Actual returned value {}", expectedValue, result);
            }


            Assert.assertEquals(result,  expectedValue);

        };

        List<Thread> threads = createThreads(THREADS, threadTask);
        startThreads(threads);
        waitUntilAllThreadsAreFinished(threads);
    }

    @Test
    public void futureNoExceptionTest() {
        Runnable threadTask = () -> {

            Integer randomInteger = getRandomIntegerFromInterval(1,1000);
            String expectedValue =  PREFIX + randomInteger;

            if (VERBOSE) {
                log.info("Before invoking execute with arg {}", randomInteger);
            }

            Future<String> future = dbe2.executeAsFuture(randomInteger); // it will NOT block until the result is available
            if (VERBOSE) {
                log.info("Doing some computation after invoking the future");
            }

            String result=null;
            try {
                result=future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            if (VERBOSE) {
                log.info("After invoking execute. Expected returned Value {}. Actual returned value {}", expectedValue, result);
            }


            Assert.assertEquals(result,  expectedValue);

        };


        List<Thread> threads = createThreads(THREADS, threadTask);
        startThreads(threads);
        waitUntilAllThreadsAreFinished(threads);
    }



    @Test
    public void monoNoExceptionTest() {
        Runnable threadTask = () -> {

            Integer randomInteger = getRandomIntegerFromInterval(1,1000);
            String expectedValue =  PREFIX + randomInteger;

            if (VERBOSE) {
                log.info("Before invoking execute with arg {}", randomInteger);
            }

            Mono<String> mono = dbe2.executeAsMono(randomInteger); // it will NOT block until the result is available
            if (VERBOSE) {
                log.info("Continue with computation after invoking the executeAsMono");
            }

            mono.subscribe(result-> {
                if (VERBOSE) {
                    log.info("Inside Mono. Expected  Value {}. Actual returned value {}", expectedValue, result);
                    Assert.assertEquals(result,  expectedValue);
                }
            });
        };


        List<Thread> threads = createThreads(THREADS, threadTask);
        startThreads(threads);

        waitUntilAllThreadsAreFinished(threads);
        // at this point all invoking threads are finished, but Mono's threads can be still running
        // consequently => force a delay
        sleepCurrentThread(4000);

    }



    //-----------------------------------------------------------------------------------------------------------------------

    private List<String> dbeCallback(List<Integer> integerList) {

        List<String> stringList = new ArrayList<>();
        for (Integer value: integerList) {
            stringList.add(PREFIX+value);
        }

        int millisecondsWaitSimulation = getRandomIntegerFromInterval(2000, 3000);
        sleepCurrentThread(millisecondsWaitSimulation);

        if (VERBOSE) {
            log.info("Callback. Simulated Exec Time {} ms.  Received args => {}. Returned {}. ", millisecondsWaitSimulation, integerList, stringList );
        }
        // to force fail in a test, uncomment this:
        stringList.set(0,"UNEXPECTED");
        return stringList;
    }


    //-----------------------------------------------------------------------------------------------------------------------



    private void startThreads(List<Thread> threads) {
        threads.forEach(Thread::start);
    }

    private void waitUntilAllThreadsAreFinished(List<Thread> threads) {
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }


    private List<Thread> createThreads(int threadsCount, Runnable task) {
        List<Thread> threads = new ArrayList<>();
        for (int threadCounter = 0; threadCounter < threadsCount; threadCounter++) {
            Thread thread = new Thread(task);
            threads.add(thread);
        }
        return threads;
    }







    public static void sleepCurrentThread(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private Integer getRandomIntegerFromInterval(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }
}
