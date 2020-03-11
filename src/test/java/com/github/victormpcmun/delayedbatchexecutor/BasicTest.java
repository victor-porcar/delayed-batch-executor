package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.sample.Product;
import com.github.victormpcmun.delayedbatchexecutor.sample.ProductDAO;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BasicTest {

    private final DelayedBatchExecutor2<Integer, Integer> dbe =
            DelayedBatchExecutor2.define(Duration.ofMillis(500), 10, this::retrieveProductsByIds);



    private final static int MAX_THREADS=224;

    private final ProductDAO productDAO = ProductDAO.productDAOSingleton;

    @Test
    public void go() {
        List<Thread> threads = createThreadsAndExecuteAsync();

        threads.forEach(Thread::start);
        //printNumberOfActiveThreads();
        threads.forEach(this::join); // wait until all threads have finished
        //printNumberOfActiveThreads();
        System.out.println("At this point all threads are finished");
        randomPause(100000, 100000); // Simulate other things
    }


    private void join(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Thread> createThreadsAndExecuteAsync() {
        List<Thread> threads = new ArrayList<>();
        for (int threadCounter = 0; threadCounter < MAX_THREADS; threadCounter++) {
            final int productId = threadCounter;
            Thread thread = new Thread(() -> {

                dbe.execute(productId);

            });
            threads.add(thread);
        }
        return threads;
    }


    private void randomPause(int millisecondsInit, int millisecondsEnd) {
        try {
            Thread.sleep(millisecondsInit + (int) (Math.random() * millisecondsEnd));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void printNumberOfActiveThreads() {
        System.out.println("ACTIVE THREADS:" + java.lang.Thread.activeCount());
    }



    private List<Integer> retrieveProductsByIds(List<Integer> params) {
        System.out.println(params.size() + "   " + Thread.currentThread().getName());
        List<Integer> result = new ArrayList<>();
        for (int i=0; i<params.size(); i++) {

            if (params.get(i)==10) {
                System.out.println("ASI ESTOY YO");
                throw new RuntimeException("ERROR");
            }
            result.add(params.get(i) + 100);
        }
        randomPause(8000, 9000);
        return result;
    }






}
