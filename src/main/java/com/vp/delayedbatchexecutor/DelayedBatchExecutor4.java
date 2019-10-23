package com.vp.delayedbatchexecutor;


import java.time.Duration;
import java.util.List;


public class DelayedBatchExecutor4<Z,A,B,C> extends DelayedBatchExecutor {

    private final DelayedParameterFunction4 userFunctionToBeInvoked;

    DelayedBatchExecutor4(Duration windowTime, int size, DelayedParameterFunction4 userFunctionToBeInvoked) {
        super(windowTime, size);
        this.userFunctionToBeInvoked=userFunctionToBeInvoked;
    }

    public Z execute(A a, B b, C c) {
        return (Z) executeWithArgs(a,b,c).getResult();
    }

    @Override
    protected  List<Object> getResultFromTupleList(TupleListArgs tupleListArgs) {
        return userFunctionToBeInvoked.apply(
                tupleListArgs.getArgsList(0),
                tupleListArgs.getArgsList(1),
                tupleListArgs.getArgsList(2)
        );
    }

}