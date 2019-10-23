package com.vp.delayedbatchexecutor;


import java.time.Duration;
import java.util.List;


public class DelayedBatchExecutor5<Z,A,B,C,D> extends DelayedBatchExecutor {

    private final DelayedParameterFunction5 userFunctionToBeInvoked;

    DelayedBatchExecutor5(Duration windowTime, int size, DelayedParameterFunction5 userFunctionToBeInvoked) {
        super(windowTime, size);
        this.userFunctionToBeInvoked=userFunctionToBeInvoked;
    }

    public Z execute(A a, B b, C c, D d) {
        return (Z) executeWithArgs(a,b,c,d).getResult();
    }

    @Override
    protected  List<Object> getResultFromTupleList(TupleListArgs tupleListArgs) {
        return userFunctionToBeInvoked.apply(
                tupleListArgs.getArgsList(0),
                tupleListArgs.getArgsList(1),
                tupleListArgs.getArgsList(2),
                tupleListArgs.getArgsList(3)
        );
    }

}