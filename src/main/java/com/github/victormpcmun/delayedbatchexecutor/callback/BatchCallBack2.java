package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.List;

/**
 * Receive as argument a List of type A and returns a List of type Z. It can be implemented as a lambda expression or method reference
 * <br>
 * <pre>
 * <b>Lambda expression</b>
 * {@code
 * DelayedBatchExecutor2<Integer,String> dbe = DelayedBatchExecutor2.define(Duration.ofMillis(50), 10, arg1List ->
 * {
 *      //arg1List is a List<String>
 *      List<Integer> result = ...
 *	    ...
 *      return result;
 *});
 *}
 * <b>Method reference</b>
 * {@code
 * DelayedBatchExecutor2<Integer,String> dbe = DelayedBatchExecutor2.define(Duration.ofMillis(50), 10, this::myBatchCallBack);
 * ...
 * List<Integer> myBatchCallBack(List<String> arg1List) {
 *      List<Integer> result = ...
 *	    ...
 *      return result;
 *}
 *}
 * </pre>
 * @author Victor Porcar
 *
 */
@FunctionalInterface
public interface BatchCallBack2<Z,A> {
    List<Z> apply(List<A> firstParam);
}