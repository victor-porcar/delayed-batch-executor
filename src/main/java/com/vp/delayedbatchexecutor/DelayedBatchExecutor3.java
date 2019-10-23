package com.vp.delayedbatchexecutor;


import java.time.Duration;
import java.util.List;


public class DelayedBatchExecutor3<Z,A,B> extends DelayedBatchExecutor {

    private final DelayedParameterFunction3 userFunctionToBeInvoked;

    DelayedBatchExecutor3(Duration windowTime, int size, DelayedParameterFunction3 userFunctionToBeInvoked) {
        super(windowTime, size);
        this.userFunctionToBeInvoked=userFunctionToBeInvoked;
    }

    public Z execute(A a, B b) {
        return (Z) executeWithArgs(a,b).getResult();
    }

    @Override
    protected  List<Object> getResultFromTupleList(TupleListArgs tupleListArgs) {
        return userFunctionToBeInvoked.apply(tupleListArgs.getArgsList(0), tupleListArgs.getArgsList(1));
    }

}