package com.vp.delayedbatchexecutor;


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

    private final CallBack2 userFunctionToBeInvoked;

    DelayedBatchExecutor2(Duration windowTime, int size, CallBack2 userFunctionToBeInvoked) {
        super(windowTime, size);
        this.userFunctionToBeInvoked=userFunctionToBeInvoked;
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

    public Z execute(A arg1) {
        Future<Z> future = executeAsync(arg1);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", e);
        }

    }


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

    public Future<Z> executeAsync(A arg1) {
        Tuple<Z> tuple = new Tuple<>(arg1);
        executeWithArgs(tuple);
        return tuple;
    }

    @Override
    protected  List<Object> getResultFromTupleList(TupleListArgs tupleListArgs) {
        return userFunctionToBeInvoked.apply(tupleListArgs.getArgsList(0));
    }

}