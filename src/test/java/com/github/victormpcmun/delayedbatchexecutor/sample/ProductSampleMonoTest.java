package com.github.victormpcmun.delayedbatchexecutor.sample;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProductSampleMonoTest {

    private final static int MAX_THREADS=120;

    private final ProductDAO productDAO = ProductDAO.productDAOSingleton;

    @Test
    public void productSampleAsync() {
        List<Thread> threads = createThreadsAndExecuteAsync();

        threads.forEach(Thread::start);
        printNumberOfActiveThreads();
        threads.forEach(this::join); // wait until all threads have finished
        printNumberOfActiveThreads();
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

                    Mono<Product> productMono = productDAO.getProductByIdMono(productId);

                    productMono.subscribe(product->
                    {

                        System.out.println(product.getDescription());
                        Assert.assertEquals(product.getDescription(),  ProductDAO.DESCRIPTION + productId);
                    });

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


}
