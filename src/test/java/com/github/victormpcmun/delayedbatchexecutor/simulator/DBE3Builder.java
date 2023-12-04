package com.github.victormpcmun.delayedbatchexecutor.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class DBE3Builder {


    private static final Logger log = LoggerFactory.getLogger(DBE3Builder.class);

    public static com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor3<String, Long,Integer> buildDBE(
            long delayedBatchExecutorWindow,
            int delayedBatchExecutorMaxSize,
            int msToCompleteDbeCallBack) {

        Duration delayedBatchExecutorWindowDuration = Duration.ofMillis(delayedBatchExecutorWindow);

        com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor3<String, Long,Integer> dbe3 = com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor3.create(
                delayedBatchExecutorWindowDuration,
                delayedBatchExecutorMaxSize /*,
				Executors.newFixedThreadPool(4),
				8192,
				true*/,
                (timestampLongList, threadNumberIntegerList) -> {
                    long now = System.currentTimeMillis();
                    SimulatorUtils.sleepCurrentThread(msToCompleteDbeCallBack);

                    long maxDurationWaiting = -1;

                    for (long timestamp:timestampLongList) {
                        long duration = now - timestamp;
                        if (duration>maxDurationWaiting) {
                            maxDurationWaiting=duration;
                        }
                    }


                    String currentThreadName = Thread.currentThread().getName();

                    log.info("DBE Callback executed in thread {}. Size of list {}. Max wait to be invoked: {} ms",
                            currentThreadName,
                            threadNumberIntegerList.size() ,
                            maxDurationWaiting);

                    List<String> result = new ArrayList<>();
                    for (int i=0; i<threadNumberIntegerList.size();i++) {
                        String resultForSecondAndThread = SimulatorUtils.concatenateInt(timestampLongList.get(i), threadNumberIntegerList.get(i));
                        result.add(resultForSecondAndThread);
                    }

                    return result;
                });

        return dbe3;

    }
}
