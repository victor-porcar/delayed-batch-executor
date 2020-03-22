package com.github.victormpcmun.delayedbatchexecutor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BatchCallBackExecutionResultTest {
/*
    @Test
    public void batchCallBackExecutionResultListWithSmallerSize() {
        List<Object> result = new ArrayList<>();
        RuntimeException runtimeException = null;
        result.add("ELEM1");
        result.add("ELEM2");
        BatchCallBackExecutionResult batchCallBackExecutionResult = new BatchCallBackExecutionResult(result, runtimeException, result.size()+10);
        Assert.assertEquals(batchCallBackExecutionResult.getReturnedResultOrNull(0), result.get(0));
        Assert.assertEquals(batchCallBackExecutionResult.getReturnedResultOrNull(1), result.get(1));
        Assert.assertNull(batchCallBackExecutionResult.getReturnedResultOrNull(11));
    }

    @Test
    public void batchCallBackExecutionResultListWithEqualSize() {
        List<Object> result = new ArrayList<>();
        RuntimeException runtimeException = null;
        result.add("ELEM1");
        result.add("ELEM2");
        BatchCallBackExecutionResult batchCallBackExecutionResult = new BatchCallBackExecutionResult(result, runtimeException, result.size());
        Assert.assertEquals(batchCallBackExecutionResult.getReturnedResultOrNull(0), result.get(0));
        Assert.assertEquals(batchCallBackExecutionResult.getReturnedResultOrNull(1), result.get(1));
    }

    @Test
    public void batchCallBackExecutionResultListWithNull() {

        RuntimeException runtimeException = null;
        BatchCallBackExecutionResult batchCallBackExecutionResult = new BatchCallBackExecutionResult(null, runtimeException, 2);
        Assert.assertNull(batchCallBackExecutionResult.getReturnedResultOrNull(0));
        Assert.assertNull(batchCallBackExecutionResult.getReturnedResultOrNull(1));
    }

    @Test
    public void batchCallBackExecutionResultListWithException() {
        RuntimeException runtimeException = new RuntimeException();
        BatchCallBackExecutionResult batchCallBackExecutionResult = new BatchCallBackExecutionResult(null, runtimeException, 2);
        Assert.assertEquals(runtimeException, batchCallBackExecutionResult.getThrownRuntimeExceptionOrNull());
    }

 */
}
