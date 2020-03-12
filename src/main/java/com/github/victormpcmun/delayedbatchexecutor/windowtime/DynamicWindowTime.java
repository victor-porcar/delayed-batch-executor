package com.github.victormpcmun.delayedbatchexecutor.windowtime;

import com.github.victormpcmun.delayedbatchexecutor.WindowTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;


public class DynamicWindowTime extends WindowTime {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private AtomicLong invocationsCounter = new AtomicLong(0);

    private Duration CHECKING_INTERVAL_DURATION = Duration.ofSeconds(5);

    private Instant lastCheckingInstant;
    private long lastCheckingInvocations;

    private Duration maxDuration;
    private int maxSize;
    private int maxConnectionsPerSecond;

    private DynamicWindowTime(Duration maxDuration, int maxSize, int maxConnectionsPerSecond) {
        this.maxDuration = maxDuration;
        this.maxSize = maxSize;
        this.maxConnectionsPerSecond=maxConnectionsPerSecond;

        this.lastCheckingInvocations=0;
        this.lastCheckingInstant=now();
    }

    public static DynamicWindowTime create(Duration maxDuration, int maxSize, int maxConnectionsPerSecond) {
        return new DynamicWindowTime(maxDuration, maxSize, maxConnectionsPerSecond);
    }

    @Override
    public void startup() {
        updateBufferedTimeoutFlux(maxDuration, maxSize);
    }

    @Override
    public synchronized  void invocation() {
        invocationsCounter.incrementAndGet();
        Instant now = now();
        Duration durationBetweenNowAndLastCheckingInstant=Duration.between(lastCheckingInstant, now);
        boolean moreThanCheckingInterval = durationBetweenNowAndLastCheckingInstant.compareTo(CHECKING_INTERVAL_DURATION) >0;
        if (moreThanCheckingInterval)  {

            long invocationsInInterval=invocationsCounter.get()-lastCheckingInvocations;
            tuneWindowTimeIfItHasTo(durationBetweenNowAndLastCheckingInstant, invocationsInInterval);
            lastCheckingInstant=now;
            lastCheckingInvocations=invocationsCounter.get();
        }
    }

    private void tuneWindowTimeIfItHasTo(Duration duration, long invocations) {
        long invocationsPerSecond = invocations / duration.getSeconds();


        long newSize = invocationsPerSecond / maxConnectionsPerSecond;
        if (newSize==0) {
            newSize=1;
        }

        if (newSize>=maxSize) {
            newSize=maxSize;
        }

        log.info("Reevaluting. Invocations Per Second {}. Sending NewDuration {}, newSize{}", invocationsPerSecond, maxDuration.toMillis(), newSize);
        updateBufferedTimeoutFlux(maxDuration, (int) newSize);
    }

    private Instant now() {
        return Instant.now();
    }
}
