package com.github.victormpcmun.delayedbatchexecutor.sample;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProductSampleAsyncTest {

    private final static int MAX_THREADS=120;

    private final ProductDAO productDAO = ProductDAO.productDAOSingleton;

    @Test
    public void productSampleAsync() {
        List<Thread> threads = createThreadsAndExecuteAsync();
        threads.forEach(Thread::start);
        threads.forEach(this::join); // wait until all threads have finished
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
                try {

                    Future<Product> futureProduct = productDAO.getProductByIdAsync(productId);
                    // DO OTHER THINGS ...
                    randomPause(100, 1000); // Simulate other things
                    // DO OTHER THINGS
                    Product product = futureProduct.get();
                    Assert.assertEquals(product.getDescription(),  ProductDAO.DESCRIPTION + productId);

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }

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
}
