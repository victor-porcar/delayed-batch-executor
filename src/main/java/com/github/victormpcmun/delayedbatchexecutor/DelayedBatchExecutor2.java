package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.callback.CallBack2;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * a Delayed Batch Executor for one argument (type A) and return type Z
 <br>
 <br>
 * Example:
 <br>
 * <pre>
 * {@code
 *
 * DelayedBatchExecutor2<Integer,String> dbe = DelayedBatchExecutor2.define(Duration.ofMillis(50), 10, this::myCallBack);
 *
 * ...
 *
 * public void logicUsingBlocking(String param1) {
 *    Integer integerResult = dbe.execute(param1); // the thread will be blocked until the result is available
 *    // compute logic with integerResult
 * }
 *
 *
 * public void logicUsingFuture(String param1) {
 *    Future<Integer> resultAsFutureInteger = dbe.executeAsFuture(param1); // the thread will not  be blocked
 *    // computer something else
 *    Integer integerResult = resultAsFutureInteger.get();  // the thread will be blocked until the result is available (if it is not at the invocation time)
 *    // compute logic with integerResult
 * }
 *
 *
 * public void logicUsingMono(String param1) {
 *    Mono<Integer> resultAsMonoInteger = dbe.executeAsMono(param1); // the thread will not  be blocked
 *    resultAsMonoInteger.subscribe(integerResult -> {
 *      // compute logic with integerResult
 *    });
 * }
 *
 *
 *
 * List<Integer> myCallBack(List<String> arg1List) {
 *      List<Integer> result = ...
 *	    ...
 *      return result;
 *}
 *}
 * </pre>
 * @author Victor Porcar
 *
 */

public class DelayedBatchExecutor2<Z,A> extends DelayedBatchExecutor {

    private final CallBack2 callback;

    /**
     * Factory method to create an instance of a Delayed Batch Executor for one argument (type A) and return type Z
     * <br>
     * @param  <Z>  the return type
     * @param  <A>  the type of the argument
     * @param  windowTime  the time of the window time, defined as a {@link Duration }
     * @param  size the max collected size.  As soon as  the number of collected parameters reaches this size, the callback method is executed
     * @param  callBack2 the method reference or lambda expression that receives a list of type A and returns a list of Type Z (see {@link CallBack2})
     * @return  an instance of {@link DelayedBatchExecutor2}
     *
     */


    public static <Z,A> DelayedBatchExecutor2<Z,A> define(Duration windowTime, int size, CallBack2<Z,A> callBack2) {
        return new DelayedBatchExecutor2<>(windowTime, size, callBack2);
    }



    /**
     * Factory method to create an instance of a Delayed Batch Executor for one argument (type A) and return type Z
     * <br>
     * @param  <Z>  the return type
     * @param  <A>  the type of the argument
     * @param  windowTime  the time of the window time, defined as a {@link Duration }
     * @param  size the max collected size.  As soon as  the number of collected parameters reaches this size, the callback method is executed
     * @param  executorService to define the pool of threads to executed the callBack method in asynchronous mode
     * @param  callBack2 the method reference or lambda expression that receives a list of type A and returns a list of Type Z (see {@link CallBack2})
     * @return  an instance of {@link DelayedBatchExecutor2}
     *
     */
    public static <Z,A> DelayedBatchExecutor2<Z,A> define(Duration windowTime, int size, ExecutorService executorService, CallBack2<Z, A> callBack2) {
        return new DelayedBatchExecutor2<>(windowTime, size, executorService, callBack2);
    }

    private DelayedBatchExecutor2(Duration windowTime, int size, ExecutorService executorService, CallBack2 callback) {
        super(windowTime, size, executorService);
        this.callback = callback;
    }

    private DelayedBatchExecutor2(Duration windowTime, int size,   CallBack2 callback) {
        super(windowTime, size);
        this.callback = callback;
    }


    /**
     * Return the result of type Z, which is obtained from the returned list of the callback method for the given parameter.
     * <br>
     * <br>
     * It will throw any {@link RuntimeException} thrown inside of the  {@link CallBack2 }
     * <br>
     * <br>
     * The invoking thread is blocked until the result is available as follows:
     * <br>
     * <br>
     * <img src="{@docRoot}/doc-files/blocking.svg" />
     * @param  arg1 value of the first argument defined for this Delayed Batch Executor
     * @return  the result (type Z)
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
     * Return a {@link Future } containing the value obtained from the returned list of the callback method for the given parameter.
     * <br>
     * <br>
     * The invoking thread is not blocked. The result will be available by invoking method {@link Future#get()} of the returned  {@link Future }, blocking the thread until the result is available if it wasn't.
     * <br>
     * <br>
       * <img src="{@docRoot}/doc-files/future.svg" />
     * <br>
     * <br>
     * If  a {@link RuntimeException} is thrown inside of the  {@link CallBack2 }, then it will be the cause of the checked Exception  {@link ExecutionException}  thrown by  {@link Future#get()} as per contract of {@link Future#get()}
     * <br>
     * <br>
     * @param  arg1 value of the first argument defined for this Delayed Batch Executor
     * @return  a {@link Future } for the result (type Z)
     *
     *d: The environment variable JAVA_HOME is not correctly set.
     */
    public Future<Z> executeAsFuture(A arg1) {
        TupleFuture<Z> tupleFuture = TupleFuture.create(arg1);
        enlistTuple(tupleFuture);
        Future<Z> future = tupleFuture.getFuture();
        return future;
    }

    /**
     * Return a {@link  <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>} publishing the value obtained from the returned list of the callback method for the given parameter.
     * <br>
     * <br>
     * The invoking thread is not blocked
     * <br>
     * <br>
     * <img src="{@docRoot}/doc-files/mono.svg" />
     * <br>
     * <br>
     * If  a {@link RuntimeException} is thrown inside of the  {@link CallBack2 }, then it will be the propagated as any {@link Runtime } throw from a {@link  <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>}
     * <br>
     * <br>
     * @param  arg1  value of the first argument defined for this Delayed Batch Executor
     * @return  a {@link  <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>} for the result (type Z)
     *
     *
     */
    public <Z> Mono<Z> executeAsMono(A arg1) {
        TupleMono<Z> tupleMono = TupleMono.create(arg1);
        enlistTuple(tupleMono);
        Mono<Z> mono = tupleMono.getMono();
        return mono;
    }


    @Override
    protected  List<Object> getResultFromTupleList( List<List<Object>> transposedTupleList) {
        List<Object> resultList = callback.apply(transposedTupleList.get(0));
        return resultList;
    }
}