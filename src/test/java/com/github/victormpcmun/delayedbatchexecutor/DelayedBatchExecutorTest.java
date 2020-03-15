package com.github.victormpcmun.delayedbatchexecutor;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayedBatchExecutorTest {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final static String PREFIX = "P";

    private final static int CONCURRENT_THREADS=10;

    private final static int MIN_MILLISECONDS_SIMULATION_DELAY_CALLBACK = 2000;
    private final static int MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK = 3000;

    private final static Duration DBE_DURATION = Duration.ofMillis(50);
    private final static Integer DBE_MAX_SIZE = 4;


    // for each integer it returns the concatenated String of constant PREFIX + integer, example: for Integer 23 it returns "P23"
    private List<String> delayedBatchExecutorCallback(List<Integer> integerList) {

        List<String> stringList = new ArrayList<>();
        for (Integer value: integerList) {
            stringList.add(PREFIX+value);
        }

        // simulate a delay of execution
        int millisecondsWaitSimulation = getRandomIntegerFromInterval(MIN_MILLISECONDS_SIMULATION_DELAY_CALLBACK, MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK);
        sleepCurrentThread(millisecondsWaitSimulation);

        log.info("Callback. Simulated Exec Time {} ms.  Received args => {}. Returned {}. ", millisecondsWaitSimulation, integerList, stringList );

        // to force the test to fail, uncomment this:
        //stringList.set(0,"UNEXPECTED");
        return stringList;
    }

    //--------------------------------------------------------------------------------------------------------------------------

    @Test
    public void blockingTest() {

        DelayedBatchExecutor2<String, Integer> dbe2 = DelayedBatchExecutor2.define(DBE_DURATION, DBE_MAX_SIZE, this::delayedBatchExecutorCallback);

        Callable<Void> callable = () -> {
            Integer randomInteger = getRandomIntegerFromInterval(1,1000);
            log.info("blockingTest=>Before invoking execute with arg {}", randomInteger);
            String expectedValue =  PREFIX + randomInteger; // the String returned by delayedBatchExecutorCallback for a given integer
            String result = dbe2.execute(randomInteger); // it will block until the result is available
            log.info("blockingTest=>After invoking execute. Expected returned Value {}. Actual returned value {}", expectedValue, result);

            Assert.assertEquals(result,  expectedValue);
            return null;
        };

        List<Future<Void>> threadsAsFutures = createThreads(CONCURRENT_THREADS, callable);
        waitUntilFinishing(threadsAsFutures);
    }


    @Test
    public void futureTest() {

        DelayedBatchExecutor2<String, Integer> dbe2 = DelayedBatchExecutor2.define(DBE_DURATION, DBE_MAX_SIZE, this::delayedBatchExecutorCallback);

        Callable<Void> callable = () -> {
            Integer randomInteger = getRandomIntegerFromInterval(1,1000);
            log.info("futureTest=>Before invoking execute with arg {}", randomInteger);

            String expectedValue =  PREFIX + randomInteger; // the String returned by delayedBatchExecutorCallback for a given integer
            Future<String> future = dbe2.executeAsFuture(randomInteger); // it will NOT block until the result is available

            log.info("futureTest=>Doing some computation after invoking executeAsFuture");

            String result=null;
            try {
                result=future.get();
                log.info("futureTest=>After invoking execute. Expected returned Value {}. Actual returned value {}", expectedValue, result);
                Assert.assertEquals(result,  expectedValue);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            return null;
        };

        List<Future<Void>> threadsAsFutures = createThreads(CONCURRENT_THREADS, callable);
        waitUntilFinishing(threadsAsFutures);
    }


    @Test
    public void monoTest() {
        DelayedBatchExecutor2<String, Integer> dbe2 = DelayedBatchExecutor2.define(DBE_DURATION, DBE_MAX_SIZE, this::delayedBatchExecutorCallback);

        AtomicInteger atomicIntegerCounter = new AtomicInteger(0);

        Callable<Void> callable = () -> {
            Integer randomInteger = getRandomIntegerFromInterval(1,1000);
            log.info("monoTest=>Before invoking execute with arg {}", randomInteger);

            String expectedValue =  PREFIX + randomInteger; // the String returned by delayedBatchExecutorCallback for a given integer
            Mono<String> mono = dbe2.executeAsMono(randomInteger); // it will NOT block the thread

            log.info("monoTest=>Continue with computation after invoking the executeAsMono");

            mono.subscribe(result-> {
                log.info("monoTest=>Inside Mono. Expected  Value {}. Actual returned value {}", expectedValue, result);
                Assert.assertEquals(result,  expectedValue);
                atomicIntegerCounter.incrementAndGet();

            });
            return null;
        };

        List<Future<Void>> threadsAsFutures = createThreads(CONCURRENT_THREADS, callable);
        waitUntilFinishing(threadsAsFutures);

        sleepCurrentThread(MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK + 1000); // wait time to allow all Mono's threads to finish
        Assert.assertEquals(CONCURRENT_THREADS, atomicIntegerCounter.get());
    }



    @Test(expected = TestRuntimeException.class)
    public void blockingExceptionTest() {
        DelayedBatchExecutor2<String, Integer> dbe2LaunchingException = DelayedBatchExecutor2.define(DBE_DURATION, DBE_MAX_SIZE, integerList -> {throw new TestRuntimeException();});

        Callable<Void> callable = () -> {
            try {
                String result = dbe2LaunchingException.execute(1);
            } catch(TestRuntimeException e) {
                log.info("blockingExceptionTest=>It is capturing successfully the exception");
                throw e;  // will be rethrow in method waitUntilFinishing
            }

            return null;
        };

        List<Future<Void>> threadsAsFutures = createThreads(CONCURRENT_THREADS, callable);
        waitUntilFinishing(threadsAsFutures);
    }



    @Test
    public void monoExceptionTest() {
        AtomicInteger atomicIntegerCounter = new AtomicInteger(0);
        DelayedBatchExecutor2<String, Integer> dbe2LaunchingException = DelayedBatchExecutor2.define(DBE_DURATION, DBE_MAX_SIZE, integerList -> {throw new TestRuntimeException();});

        Callable<Void> callable = () -> {

            Mono<String> mono = dbe2LaunchingException.executeAsMono(1); // it will NOT block the thread

            mono.doOnError( TestRuntimeException.class, e ->
                { log.info("monoExceptionTest=>Successfully processed the exception");
                    atomicIntegerCounter.incrementAndGet();
                }).subscribe(result->log.info("monoExceptionTest=>This should never be printed:" + result));
            return null;
        };

        List<Future<Void>> threadsAsFutures = createThreads(CONCURRENT_THREADS, callable);
        waitUntilFinishing(threadsAsFutures);

        sleepCurrentThread(MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK + 1000); // wait time to allow all Mono's threads to finish
        Assert.assertEquals(CONCURRENT_THREADS, atomicIntegerCounter.get());
    }


    @Test(expected = TestRuntimeException.class)
    public void futureExceptionTest() {
        DelayedBatchExecutor2<String, Integer> dbe2LaunchingException = DelayedBatchExecutor2.define(DBE_DURATION, DBE_MAX_SIZE, integerList -> {throw new TestRuntimeException();});

        Callable<Void> callable = () -> {
            Future<String> future = dbe2LaunchingException.executeAsFuture(1); // it will NOT block until the result is available

            String result=null;
            try {
                result=future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                RuntimeException actualRuntimeLaunched = (RuntimeException) e.getCause();
                if (actualRuntimeLaunched instanceof TestRuntimeException) {
                    log.info("futureExceptionTest=>It is capturing successfully the exception");
                }

                throw actualRuntimeLaunched;
            }
            return null;
        };

        List<Future<Void>> threadsAsFutures = createThreads(CONCURRENT_THREADS, callable);
        waitUntilFinishing(threadsAsFutures);
    }




    @Test(expected = TestRuntimeException.class)
    public void changeConfigTest() {
        DelayedBatchExecutor2<String, Integer> dbe2LaunchingException = DelayedBatchExecutor2.define(DBE_DURATION, DBE_MAX_SIZE, integerList -> {throw new TestRuntimeException();});

        Callable<Void> callable = () -> {
            Future<String> future = dbe2LaunchingException.executeAsFuture(1); // it will NOT block until the result is available

            String result=null;
            try {
                result=future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                RuntimeException actualRuntimeLaunched = (RuntimeException) e.getCause();
                if (actualRuntimeLaunched instanceof TestRuntimeException) {
                    log.info("futureExceptionTest=>It is capturing successfully the exception");
                }

                throw actualRuntimeLaunched;
            }
            return null;
        };

        List<Future<Void>> threadsAsFutures = createThreads(CONCURRENT_THREADS, callable);
        waitUntilFinishing(threadsAsFutures);
    }




    @Test
    public void changeConfigParamTest() {

        int windowTime1=5;
        int windowTime2=2;
        int size=20;

        DelayedBatchExecutor2<String, Integer> dbe2 = DelayedBatchExecutor2.define(Duration.ofSeconds(windowTime1), size, this::delayedBatchExecutorCallback);
        log.info(dbe2.toString());

        int value1=20;
        int value2=40;

        Future<String> futureResult1 = dbe2.executeAsFuture(value1);
        log.info(dbe2.toString());
        dbe2.updateConfig(Duration.ofSeconds(windowTime2), size+10);
        log.info(dbe2.toString());
        Future<String> futureResult2 = dbe2.executeAsFuture(value2);

        try {

            String result1=futureResult1.get();
            String result2=futureResult2.get();

            long result1DelayedTime=((TupleFuture<String>) futureResult1 ).getDelayedTime().toMillis();
            long result2DelayedTime=((TupleFuture<String>) futureResult2 ).getDelayedTime().toMillis();

            log.info("RESULT1 {} - DelayedTime: {}" , result1, result1DelayedTime);
            log.info("RESULT2 {} - DelayedTime: {}" , result2, result2DelayedTime);

            Assert.assertEquals(result1, PREFIX + value1);
            Assert.assertEquals(result2, PREFIX + value2);

            Assert.assertTrue(result1DelayedTime>windowTime1*1000);
            Assert.assertTrue(result2DelayedTime>windowTime2*1000);

            log.info(dbe2.toString());

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    @Test(expected = TimeoutException.class)
    public void futureTimeOutTest() throws InterruptedException, ExecutionException, TimeoutException {
        DelayedBatchExecutor2<String, Integer> dbe2 = DelayedBatchExecutor2.define(Duration.ofMillis(2000), 2, resultList-> {return new ArrayList<>();});

        Future<String> futureResult1 = dbe2.executeAsFuture(1);
        futureResult1.get(1500, TimeUnit.MILLISECONDS);
    }


    //-----------------------------------------------------------------------------------------------------------------------
    //-----------------------------------------------------------------------------------------------------------------------

    private void waitUntilFinishing(List<Future<Void>> threads) {
        for (Future future: threads) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw (RuntimeException) e.getCause();
            }
        }
    }


    private List<Future<Void>> createThreads(int threadsCount, Callable callable) {
        ExecutorService es = Executors.newFixedThreadPool(threadsCount);
        List<Future<Void>> threads = new ArrayList<>();
        for (int threadCounter = 0; threadCounter < threadsCount; threadCounter++) {
            threads.add(es.submit(callable));
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
