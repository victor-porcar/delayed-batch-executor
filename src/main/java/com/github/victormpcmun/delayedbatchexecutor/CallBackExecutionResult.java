package com.github.victormpcmun.delayedbatchexecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CallBackExecutionResult {
    RuntimeException runtimeException;
    List resizedList;

    public CallBackExecutionResult(List result, RuntimeException runtimeException, int size) {
        this.runtimeException = runtimeException;
        this.resizedList= resizeListFillingWithNullsIfNecessary(result, size);
    }

    public RuntimeException getThrownRuntimeExceptionOrNull() {
        return runtimeException;
    }

    public Object getReturnedResultOrNull(int position) {
        return resizedList.get(position);
    }

    private <T> List<T> resizeListFillingWithNullsIfNecessary(List<T> list, int size) {
        if (list==null) {
            list= Collections.nCopies(size,  null);
        } else if (list.size()<size) {
            list = new ArrayList(list); // make it mutable in case it isn't
            list.addAll(Collections.nCopies(size-list.size(),null));
        }
        return list;
    }
}
