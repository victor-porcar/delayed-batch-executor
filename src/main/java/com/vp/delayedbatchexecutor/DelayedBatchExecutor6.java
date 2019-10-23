package com.vp.delayedbatchexecutor;


import java.time.Duration;
import java.util.List;


public class DelayedBatchExecutor6<Z,A,B,C,D,E> extends DelayedBatchExecutor {

    private final DelayedParameterFunction6 userFunctionToBeInvoked;

    DelayedBatchExecutor6(Duration windowTime, int size, DelayedParameterFunction6 userFunctionToBeInvoked) {
        super(windowTime, size);
        this.userFunctionToBeInvoked=userFunctionToBeInvoked;
    }

    public Z execute(A a, B b, C c, D d, E e) {
        return (Z) executeWithArgs(a,b,c,d,e).getResult();
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