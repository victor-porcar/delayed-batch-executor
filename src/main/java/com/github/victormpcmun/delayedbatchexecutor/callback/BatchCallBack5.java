package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.List;

/**
 * Receive as argument four Lists of type A,B,C,D and returns a List of type Z. It can be implemented as a lambda expression or method reference
 * <br>
 * <br>
 * <pre>
 * <b>Lambda expression</b>
 * {@code
 * DelayedBatchExecutor5<String,Integer,Integer,Integer,Integer> dbe = DelayedBatchExecutor5.create(Duration.ofMillis(50), 10, (arg1List, arg2List, arg3list,arg4List) ->
 * {
 *      //arg1List,arg2List,arg3List and arg4List are List<Integer>
 *      List<String> result = ...
 *	    ...
 *      return result;
 *});
 *}
 * <b>Method reference</b>
 * {@code
 * DelayedBatchExecutor5<String,Integer,Integer,Integer,Integer> dbe = DelayedBatchExecutor5.create(Duration.ofMillis(50), 10, this::myBatchCallBack);
 * ...
 * List<String> myBatchCallBack(List<Integer> arg1List, List<Integer> arg2List, List<Integer> arg3List, List<Integer> arg4List) {
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
public interface BatchCallBack5<Z,A,B,C,D> {
    List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam);
}