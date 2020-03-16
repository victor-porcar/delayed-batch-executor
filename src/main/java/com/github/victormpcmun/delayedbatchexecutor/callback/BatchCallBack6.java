package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.List;

/**
 * Receive as argument four Lists of type A,B,C,D,E and returns a List of type Z. It can be implemented as a lambda expression or method reference
 * <br>
 * <br>
 * <pre>
 * <b>Lambda expression</b>
 * {@code
 * DelayedBatchExecutor6<String,Integer,Integer,Integer,Integer,Integer> dbe = DelayedBatchExecutor6.create(Duration.ofMillis(50), 10, (arg1List, arg2List, arg3list,arg4List,arg5List) ->
 * {
 *      //arg1List,arg2List,arg3List,arg4List and arg5List are List<Integer>
 *      List<String> result = ...
 *	    ...
 *      return result;
 *});
 *}
 * <b>Method reference</b>
 * {@code
 * DelayedBatchExecutor5<String,Integer,Integer,Integer,Integer,Integer> dbe = DelayedBatchExecutor5.create(Duration.ofMillis(50), 10, this::myBatchCallBack);
 * ...
 * List<String> myBatchCallBack(List<Integer> arg1List, List<Integer> arg2List, List<Integer> arg3List, List<Integer> arg4List, List<Integer> arg5List) {
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
public interface BatchCallBack6<Z,A,B,C,D,E> {
    List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam, List<E> fifthParam);
}