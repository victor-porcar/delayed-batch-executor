package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.List;

/**
 * Receive as argument two Lists of type A and B and returns a List of type Z. It can be implemented as a lambda expression or method reference
 * <br>
 * <br>
 * <pre>
 * <b>Lambda expression</b>
 * {@code
 * DelayedBatchExecutor3<String,Integer> dbe = DelayedBatchExecutor3.define(Duration.ofMillis(50), 10, (arg1List, arg2List) ->
 * {
 *      //arg1List and arg2List are List<Integer>
 *      List<String> result = ...
 *	    ...
 *      return result;
 *});
 *}
 * <b>Method reference</b>
 * {@code
 * DelayedBatchExecutor3<String,Integer,Integer> dbe = DelayedBatchExecutor3.define(Duration.ofMillis(50), 10, this::myBatchCallBack);
 * ...
 * List<String> myBatchCallBack(List<Integer> arg1List, List<Integer> arg2List) {
 *      List<String> result = ...
 *	    ...
 *      return result;
 *}
 *}
 * </pre>
 * @author Victor Porcar
 *
 */
@FunctionalInterface
public interface BatchCallBack3<Z,A,B> {
    List<Z> apply(List<A> firstParam, List<B> secondParam);
}