package com.vp.delayedbatchexecutor;


import java.time.Duration;
import java.util.List;


public class DelayedBatchExecutor2<Z,A> extends DelayedBatchExecutor {

    private final DelayedParameterFunction2 userFunctionToBeInvoked;

    DelayedBatchExecutor2(Duration windowTime, int size, DelayedParameterFunction2 userFunctionToBeInvoked) {
        super(windowTime, size);
        this.userFunctionToBeInvoked=userFunctionToBeInvoked;
    }

    public Z execute(A a) {
        return (Z) executeWithArgs(a).getResult();
    }

    @Override
    protected  List<Object> getResultFromTupleList(TupleListArgs tupleListArgs) {
        return userFunctionToBeInvoked.apply(tupleListArgs.getArgsList(0));
    }

}