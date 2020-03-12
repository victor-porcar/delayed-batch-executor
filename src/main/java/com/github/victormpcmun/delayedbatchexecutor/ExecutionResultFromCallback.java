package com.github.victormpcmun.delayedbatchexecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExecutionResultFromCallback {
    RuntimeException runtimeException;
    List resizedList;


    public ExecutionResultFromCallback(List result, RuntimeException runtimeException, int size) {
        this.runtimeException = runtimeException;
        this.resizedList= resizeListFillingWithNullsIfNecessary(result, size);
    }

    public boolean runtimeExceptionLaunched() {
        return runtimeException!=null;
    }

    public RuntimeException getRuntimeException() {
        return runtimeException;
    }


    public Object getPositionResult(int position) {
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
