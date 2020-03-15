package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.callback.BatchCallBack2;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Delayed Batch Executor for one argument of type A and return type Z
 * <br>
 * <pre>
 * {@code
 * DelayedBatchExecutor2<Integer,String> dbe = DelayedBatchExecutor2.define(Duration.ofMillis(50), 10, this::myBatchCallback);
 *
 * ...
 *
 * public void usingDelayedBatchExecutor(String param1) {
 *
 *    // using blocking behaviour
 *    Integer integerResult = dbe.execute(param1); // the thread will be blocked until the result is available
 *    // compute with integerResult
 *
 *
 *    // using Future
 *    Future<Integer> resultAsFutureInteger = dbe.executeAsFuture(param1); // the thread will not  be blocked
 *    // compute something else
 *    Integer integerResult = resultAsFutureInteger.get();  // Blocks the thread if necessary for the computation to complete, and then retrieves its result.
 *    // compute with integerResult
 *
 *
 *    // using Mono
 *    Mono<Integer> monoResult = dbe.executeAsMono(param1); // the thread will not  be blocked
 *    // compute something else
 *    monoResult.subscribe(integerResult -> {
 *     // compute with integerResult
 *    }
 * }
 *
 * ...
 *
 * List<Integer> myBatchCallback(List<String> arg1List) {
 *   List<Integer> result = ...
 *   ...
 *   return result;
 *}
 *}
 * </pre>
 * @author Victor Porcar
 *
 */

public class DelayedBatchExecutor2<Z,A> extends DelayedBatchExecutor {

    public static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = DelayedBatchExecutor.DEFAULT_FIXED_THREAD_POOL_COUNTER; // this definitions is redundant, but javadoc seems to require it
    public static final int DEFAULT_BUFFER_QUEUE_SIZE = DelayedBatchExecutor.DEFAULT_BUFFER_QUEUE_SIZE; // this definitions is redundant, but javadoc seems to require it

    private final BatchCallBack2<Z,A> batchCallBack;

    /**
     * Factory method to create an instance of a Delayed Batch Executor for one argument of type A and return type Z
     * <br>
     * <br>
     * -It uses as a default ExecutorService:  {@link java.util.concurrent.Executors#newFixedThreadPool(int)} with the following number of threads {@value #DEFAULT_FIXED_THREAD_POOL_COUNTER}
     * <br>
     * -It uses as a default bufferQueueSize value {@value #DEFAULT_BUFFER_QUEUE_SIZE}
     * <br>
     * @param  <Z>  the return type
     * @param  <A>  the type of the argument
     * @param  duration  the time of the window time, defined as {@link Duration }.
     * @param  size the max collected size.  As soon as  the count of collected parameters reaches this size, the batchCallBack method is executed
     * @param  batchCallback2 the method reference or lambda expression that receives a list of type A and returns a list of Type Z (see {@link BatchCallBack2})
     * @return  an instance of {@link DelayedBatchExecutor2}
     *
     */


    public static <Z,A> DelayedBatchExecutor2<Z,A> create(Duration duration, int size, BatchCallBack2<Z,A> batchCallback2) {
        return new DelayedBatchExecutor2<>(duration, size, getDefaultExecutorService(), getDefaultBufferQueueSize(), batchCallback2);
    }


    /**
     * Factory method to create an instance of a Delayed Batch Executor for one argument of type A and return type Z
     * <br>
     * @param  <Z>  the return type
     * @param  <A>  the type of the argument
     * @param  duration  the time of the window time, defined as {@link Duration }.
     * @param  size the max collected size.  As soon as  the count of collected parameters reaches this size, the batchCallBack method is executed
     * @param  executorService to define the pool of threads to executed the batchCallBack method in asynchronous mode
     * @param  bufferQueueSize max size of the internal queue to store values.
     * @param  batchCallback2 the method reference or lambda expression that receives a list of type A and returns a list of Type Z (see {@link BatchCallBack2})
     * @return  an instance of {@link DelayedBatchExecutor2}
     *
     */

    public static <Z,A> DelayedBatchExecutor2<Z,A> create(Duration duration, int size, ExecutorService executorService, int bufferQueueSize, BatchCallBack2<Z, A> batchCallback2) {
        return new DelayedBatchExecutor2<>(duration, size, executorService, bufferQueueSize, batchCallback2);
    }



    private DelayedBatchExecutor2(Duration duration, int size, ExecutorService executorService, int bufferQueueSize, BatchCallBack2<Z,A> batchCallBack) {
        super(duration, size , executorService, bufferQueueSize);
        this.batchCallBack = batchCallBack;
    }

    /**
     * Return the result of type Z (blocking the thread until the result is available), which is obtained from the returned list of the batchCallBack method for the given parameter.
     * <br>
     * <br>
     * It will throw any {@link RuntimeException} thrown inside of the  {@link BatchCallBack2 }
     * <br>
     * It will throw a {@link RuntimeException} if the internal buffer Queue of this Delayed Batch Executor is full.
     * <br>
     * The invoking thread is blocked until the result is available
     * <br>
     * <br>
     * <img src="{@docRoot}/doc-files/blocking.svg" alt="blocking">
     * @param  arg1 value of the first argument  for this Delayed Batch Executor
     * @return  the result of type Z
     *
     *
     */
    public Z execute(A arg1) {
        Future<Z> future = executeAsFuture(arg1);
        try {
            return future.get();
        }  catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", e);
        }
    }

    /**
     * Return a {@link Future } containing the corresponding value from the returned list of the batchCallBack method for the given parameter.
     * <br>
     * <br>
     * The invoking thread is not blocked. The result will be available by invoking method {@link Future#get()} of the returned  {@link Future }.
     * It blocks the thread until the result is available if it wasn't.
     * <br>
     * <br>
       * <img src="{@docRoot}/doc-files/future.svg"  alt ="future">
     * <br>
     * It will throw a {@link RuntimeException} if the internal buffer Queue of this Delayed Batch Executor is full.
     * <br>
     * If  a {@link RuntimeException} is thrown inside of the  {@link BatchCallBack2 }, then it will be the cause of the checked Exception  {@link ExecutionException}  thrown by  {@link Future#get()} as per contract of {@link Future#get()}
     * <br>
     * <br>
     * @param  arg1 value of the first argument defined for this Delayed Batch Executor
     * @return  a {@link Future } for the result of type Z
     *
     */
    public Future<Z> executeAsFuture(A arg1) {
        TupleFuture<Z> tupleFuture = TupleFuture.create(arg1);
        enlistTuple(tupleFuture);
        Future<Z> future = tupleFuture.getFuture();
        return future;
    }

    /**
     * Return a  <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>, publishing the value obtained from the returned list of the batchCallBack method for the given parameter.
     * <br>
     * <br>
     * The invoking thread is not blocked
     * <br>
     * <br>
     * <img src="{@docRoot}/doc-files/mono.svg"  alt="mono">
     * <br>
     * It will throw a {@link RuntimeException} if the internal buffer Queue of this Delayed Batch Executor is full.
     * <br>
     * If  a {@link RuntimeException} is thrown inside of the  {@link BatchCallBack2 }, then it will be the propagated as any {@link RuntimeException } thrown from <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>
     * <br>
     * <br>
     * @param  arg1  value of the first argument defined for this Delayed Batch Executor
     * @return  a <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a> for the result of type Z
     *
     *
     */
    public Mono<Z> executeAsMono(A arg1) {
        TupleMono<Z> tupleMono = TupleMono.create(arg1);
        enlistTuple(tupleMono);
        Mono<Z> mono = tupleMono.getMono();
        return mono;
    }


    @Override
    protected  List<Object> getResultListFromBatchCallBack(List<List<Object>> transposedTupleList) {
        List<Object> resultList = (List<Object>) batchCallBack.apply(
                (List<A>) transposedTupleList.get(0)
        );
        return resultList;
    }
}