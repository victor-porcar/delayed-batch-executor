package com.github.victormpcmun.delayedbatchexecutor.sample;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ProductSampleTest {

    private final static int MAX_THREADS=120;

    private final ProductDAO productDAO = ProductDAO.productDAOSingleton;

    @Test
    public void productSampleTest() {
        List<Thread> threads = createThreads();
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

    private List<Thread> createThreads() {
        List<Thread> threads = new ArrayList<>();
        for (int threadCounter = 0; threadCounter < MAX_THREADS; threadCounter++) {
            final int productId = threadCounter;
            Thread thread = new Thread(() -> {
                Product product = productDAO.getProductById(productId);
                Assert.assertEquals(product.getDescription(), ProductDAO.DESCRIPTION + productId);
            });
            threads.add(thread);
        }
        return threads;
    }
}
