package com.github.victormpcmun.delayedbatchexecutor;



import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleFuture;

import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleMono;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A subclass of a DelayedBatchExecutor for one argument.
 *
 * @author Victor Porcar
 *
 */

public class DelayedBatchExecutor2<Z,A> extends DelayedBatchExecutor {

    private final CallBack2 handler;



    @FunctionalInterface
    public interface CallBack2<Z,A> {
        List<Z> apply(List<A> firstParam);
    }

    /**
     * Creates an instance of a DelayedBatchExecutor2&lt;Z,A&gt;,
     * which is a subclass of a DelayedBatchExecutor for one argument.
     * <p>
     * @param  <Z>  the return type
     * @param  <A>  the type of the argument
     * @param  windowTime  the time of the window time, defined as a java.time.Duration
     * @param  size the maximum number of parameters, whenever the number of parameters reaches this limit, the window time is close and the callback method is executed
     * @param  callBack the method that will receive a list of type A and returns a list of Type Z, which must have he size of the list received
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */


    public static <Z,A> DelayedBatchExecutor2<Z,A> define(Duration windowTime, int size, CallBack2<Z,A> callBack) {
        return new DelayedBatchExecutor2<>(windowTime, size, callBack);
    }



    private DelayedBatchExecutor2(Duration windowTime, int size, CallBack2 handler) {
        super(windowTime, size);
        this.handler=handler;
    }

    /**
     * returns the result of execution of the callback method of the DelayedBatchExecutor for the given parameter.
     * It blocks the execution of the thread until the result is available
     * which means that it could take in the worst case the windowTime defined for the DelayedBatchExecutor.
     *
     * <p>
     *
     * @param  arg1 an instance of the  argument defined for the DelayedBatchExecutor
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */




    /**
     * returns a Future of the result of execution of the callback method of the DelayedBatchExecutor for the given parameter.
     * It does NOT block the execution of the thread.
     *
     * <p>
     *
     * @param  arg1 an instance of the  argument defined for the DelayedBatchExecutor
     * @return  a Future of of type Z
     *
     * @author Victor Porcar
     *
     */


    public Z execute(A arg1) {
        Future<Z> future = executeAsFuture(arg1);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", e);
        }
    }

    public Future<Z> executeAsFuture(A arg1) {
        TupleFuture<Z> tupleFuture = TupleFuture.create(arg1);
        enlistTuple(tupleFuture);
        Future<Z> future = tupleFuture.getFuture();
        return future;
    }

    public <Z> Mono<Z> executeAsMono(A arg1) {
        TupleMono<Z> tupleMono = TupleMono.create(arg1);
        enlistTuple(tupleMono);
        Mono<Z> mono = tupleMono.getMono();
        return mono;
    }


    @Override
    protected  List<Object> getResultFromTupleList( List<List<Object>> transposedTupleList) {
        List<Object> resultList = handler.apply(transposedTupleList.get(0));
        return resultList;
    }
}