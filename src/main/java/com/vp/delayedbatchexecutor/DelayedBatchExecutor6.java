package com.vp.delayedbatchexecutor;


import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A subclass of a DelayedBatchExecutor for five arguments.
 * @author Victor Porcar
 *
 */
public class DelayedBatchExecutor6<Z,A,B,C,D,E> extends DelayedBatchExecutor {

    private final CallBack6 userFunctionToBeInvoked;

    DelayedBatchExecutor6(Duration windowTime, int size, CallBack6 userFunctionToBeInvoked) {
        super(windowTime, size);
        this.userFunctionToBeInvoked=userFunctionToBeInvoked;
    }


    /**
     * returns the result of execution of the callback method of the DelayedBatchExecutor for the given parameters.
     * It blocks the execution of the thread until the result is available
     * which means that it could take in the worst case the windowTime defined for the DelayedBatchExecutor.
     *
     * <p>
     *
     * @param  arg1 an instance of the first  argument defined for the DelayedBatchExecutor
     * @param  arg2 an instance of the second argument defined for the DelayedBatchExecutor
     * @param  arg3 an instance of the third argument defined for the DelayedBatchExecutor
     * @param  arg4 an instance of the fourth argument defined for the DelayedBatchExecutor
     * @param  arg5 an instance of the fifth argument defined for the DelayedBatchExecutor
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */
    public Z execute(A arg1, B arg2, C arg3, D arg4, E arg5) {
        Future<Z> future = executeAsFuture(arg1,arg2,arg3,arg4,arg5);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException ee) {
            throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", ee);
        }
    }


    /**
     * returns a Future of the result of execution of the callback method of the DelayedBatchExecutor for the given parameters.
     * It does NOT block the execution of the thread.
     *
     * <p>
     *
     * @param  arg1 an instance of the first  argument defined for the DelayedBatchExecutor
     * @param  arg2 an instance of the second argument defined for the DelayedBatchExecutor
     * @param  arg3 an instance of the third argument defined for the DelayedBatchExecutor
     * @param  arg4 an instance of the fourth argument defined for the DelayedBatchExecutor
     * @param  arg5  an instance of the fifth argument defined for the DelayedBatchExecutor
     * @return  a Future of of type Z
     *
     * @author Victor Porcar
     *
     */
    public Future<Z> executeAsFuture(A arg1, B arg2, C arg3, D arg4, E arg5) {
        Tuple<Z> tuple = new Tuple<>(arg1,arg2,arg3,arg4,arg5);
        executeWithArgs(tuple);
        return tuple;
    }

    @Override
    protected  List<Object> getResultFromTupleList(TupleListArgs tupleListArgs) {
        return userFunctionToBeInvoked.apply(
                tupleListArgs.getArgsList(0),
                tupleListArgs.getArgsList(1),
                tupleListArgs.getArgsList(2),
                tupleListArgs.getArgsList(3),
                tupleListArgs.getArgsList(4)
        );
    }
}