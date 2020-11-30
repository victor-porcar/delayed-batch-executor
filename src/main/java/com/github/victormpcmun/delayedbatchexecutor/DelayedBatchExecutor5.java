package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Delayed Batch Executor for four arguments (of types A,B,C and D) and return
 * type Z <br>
 * 
 * <pre>
 * {@code
 * DelayedBatchExecutor5<String,Integer,Integer,Integer,Integer> dbe = DelayedBatchExecutor5.create(Duration.ofMillis(50), 10, this::myBatchCallback);
 *
 * ...
 *
 * public void usingDelayedBatchExecutor(Integer param1, Integer param2, Integer param3, Integer param4) {
 *
 *    // using blocking behaviour
 *    String stringResult1 = dbe.execute(param1, param2, param3,param4); // the thread will be blocked until the result is available
 *    // compute with stringResult1
 *
 *
 *    // using Future
 *    Future<String> resultAsFutureString = dbe.executeAsFuture(param1, param2, param3, param4); // the thread will not  be blocked
 *    // compute something else
 *    String stringResult2 = resultAsFutureString.get();  // Blocks the thread if necessary for the computation to complete, and then retrieves its result.
 *    // compute with stringResult2
 *
 *
 *    // using Mono
 *    Mono<String> monoResult = dbe.executeAsMono(param1, param2, param3, param4); // the thread will not  be blocked
 *    // compute something else
 *    monoResult.subscribe(stringResult -> {
 *     // compute with stringResult
 *    });
 * }
 *
 * ...
 *
 * List<String> myBatchCallback(List<Integer> arg1List, List<Integer> arg2List, List<Integer> arg3List, List<Integer> arg4List) {
 *   List<String> result = ...
 *   ...
 *   return result;
 *}
 *}
 * </pre>
 * 
 * @author Victor Porcar
 *
 */

public class DelayedBatchExecutor5<Z, A, B, C, D> extends DelayedBatchExecutor {

	/**
	 * Receive as argument four Lists of type A,B,C,D and returns a List of type Z.
	 * It can be implemented as a lambda expression or method reference <br>
	 * <br>
	 * 
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
	 * 
	 * @author Victor Porcar
	 *
	 */
	@FunctionalInterface
	public interface BatchCallBack5<Z, A, B, C, D> {
		List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam);
	}

	private final BatchCallBack5<Z, A, B, C, D> batchCallBack;

	/**
	 * Factory method to create an instance of a Delayed Batch Executor for two
	 * arguments (of types A,B,C and D) and return type Z. Similar to
	 * {@link DelayedBatchExecutor5#create(Duration, int, ExecutorService, int, boolean, BatchCallBack5)}
	 * defaulting to: <br>
	 * <br>
	 * -executorService: the one returned by static method
	 * {@link #getDefaultExecutorService()} <br>
	 * -bufferQueueSize: the value of constant {@link #DEFAULT_BUFFER_QUEUE_SIZE}
	 * <br>
	 * -removeDuplicates:true <br>
	 * 
	 * @param <Z>            the return type
	 * @param <A>            the type of the first argument
	 * @param <B>            the type of the second argument
	 * @param <C>            the type of the third argument
	 * @param <D>            the type of the fourth argument
	 * @param duration       the time window, defined as {@link Duration }.
	 * @param size           the max collected size. As soon as the count of
	 *                       collected parameters reaches this size, the
	 *                       batchCallBack method is executed
	 * @param batchCallback5 the method reference or lambda expression that receives
	 *                       a list of type A and returns a list of Type Z (see
	 *                       {@link BatchCallBack5})
	 * @return an instance of {@link DelayedBatchExecutor5}
	 *
	 */
	public static <Z, A, B, C, D> DelayedBatchExecutor5<Z, A, B, C, D> create(Duration duration, int size,
			BatchCallBack5<Z, A, B, C, D> batchCallback5) {
		return new DelayedBatchExecutor5<>(duration, size, null, DEFAULT_BUFFER_QUEUE_SIZE, true, batchCallback5);
	}

	/**
	 * Factory method to create an instance of a Delayed Batch Executor for two
	 * arguments (of types A,B,C and D) and return type Z <br>
	 * 
	 * @param <Z>              the return type
	 * @param <A>              the type of the first argument
	 * @param <B>              the type of the second argument
	 * @param <C>              the type of the third argument
	 * @param <D>              the type of the fourth argument
	 * @param duration         the time window, defined as {@link Duration }.
	 * @param size             the max collected size. As soon as the count of
	 *                         collected parameters reaches this size, the
	 *                         batchCallBack method is executed
	 * @param executorService  to define the pool of threads to executed the
	 *                         batchCallBack method in asynchronous mode
	 * @param bufferQueueSize  max size of the internal queue to buffer values
	 * @param removeDuplicates if true then duplicated arguments from execute*(...)
	 *                         methods are not passed to the batchCallBack
	 *                         (considering same {@link Object#hashCode()} and being
	 *                         {@link Object#equals(Object)})
	 * @param batchCallback5   the method reference or lambda expression that
	 *                         receives a list of type A and returns a list of Type
	 *                         Z (see {@link BatchCallBack5})
	 * @return an instance of {@link DelayedBatchExecutor5}
	 *
	 */
	public static <Z, A, B, C, D> DelayedBatchExecutor5<Z, A, B, C, D> create(Duration duration, int size,
			ExecutorService executorService, int bufferQueueSize, boolean removeDuplicates,
			BatchCallBack5<Z, A, B, C, D> batchCallback5) {
		return new DelayedBatchExecutor5<>(duration, size, executorService, bufferQueueSize, removeDuplicates,
				batchCallback5);
	}

	private DelayedBatchExecutor5(Duration duration, int size, ExecutorService executorService, int bufferQueueSize,
			boolean removeDuplicates, BatchCallBack5<Z, A, B, C, D> batchCallBack) {
		super(duration, size, executorService, bufferQueueSize, removeDuplicates);
		this.batchCallBack = batchCallBack;
	}

	/**
	 * Return the result of type Z (blocking the thread until the result is
	 * available), which is obtained from the returned list of the batchCallBack
	 * method for the given argument <br>
	 * <br>
	 * It will throw any {@link RuntimeException} thrown inside of the
	 * {@link BatchCallBack5 } <br>
	 * It will throw a {@link RuntimeException} if the internal buffer Queue of this
	 * Delayed Batch Executor is full. <br>
	 * <br>
	 * <img src="{@docRoot}/doc-files/blocking.svg" alt="blocking">
	 * 
	 * @param arg1 value of the first argument of type A defined for this Delayed
	 *             Batch Executor
	 * @param arg2 value of the second argument of type B defined for this Delayed
	 *             Batch Executor
	 * @param arg3 value of the third argument of type C defined for this Delayed
	 *             Batch Executor
	 * @param arg4 value of the fourth argument of type D defined for this Delayed
	 *             Batch Executor
	 * @return the result of type Z
	 *
	 *
	 */
	public Z execute(A arg1, B arg2, C arg3, D arg4) {
		TupleBlocking<Z> tupleBlocking = new TupleBlocking<>(arg1, arg2, arg3, arg4);
		enlistTuple(tupleBlocking);
		Z value = tupleBlocking.getValueBlocking();
		return value;
	}

	/**
	 * Return a {@link Future } containing the corresponding value from the returned
	 * list of the batchCallBack method for the given argument <br>
	 * <br>
	 * The invoking thread is not blocked. The result will be available by invoking
	 * method {@link Future#get()} of the {@link Future }. This method will block
	 * the thread until the result is available <br>
	 * <br>
	 * <img src="{@docRoot}/doc-files/future.svg" alt ="future"> <br>
	 * It will throw a {@link RuntimeException} if the internal buffer Queue of this
	 * Delayed Batch Executor is full. <br>
	 * If a {@link RuntimeException} is thrown inside of the {@link BatchCallBack5
	 * }, then it will be the cause of the checked Exception
	 * {@link ExecutionException} thrown by {@link Future#get()} as per contract of
	 * {@link Future#get()} <br>
	 * <br>
	 * 
	 * @param arg1 value of the first argument of type A defined for this Delayed
	 *             Batch Executor
	 * @param arg2 value of the second argument of type B defined for this Delayed
	 *             Batch Executor
	 * @param arg3 value of the third argument of type C defined for this Delayed
	 *             Batch Executor
	 * @param arg4 value of the fourth argument of type D defined for this Delayed
	 *             Batch Executor
	 * @return a {@link Future } for the result of type Z
	 *
	 */
	public Future<Z> executeAsFuture(A arg1, B arg2, C arg3, D arg4) {
		TupleFuture<Z> tupleFuture = new TupleFuture<>(arg1, arg2, arg3, arg4);
		enlistTuple(tupleFuture);
		Future<Z> future = tupleFuture.getFuture();
		return future;
	}

	/**
	 * Return a <a href=
	 * "https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>,
	 * publishing the value obtained from the returned list of the batchCallBack
	 * method for the given parameter. <br>
	 * <br>
	 * The invoking thread is not blocked <br>
	 * <br>
	 * <img src="{@docRoot}/doc-files/mono.svg" alt="mono"> <br>
	 * It will throw a {@link RuntimeException} if the internal buffer Queue of this
	 * Delayed Batch Executor is full. <br>
	 * If a {@link RuntimeException} is thrown inside of the {@link BatchCallBack5},
	 * then it will be the propagated as any {@link RuntimeException } thrown from
	 * <a href=
	 * "https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>
	 * <br>
	 * <br>
	 * 
	 * @param arg1 value of the first argument of type A defined for this Delayed
	 *             Batch Executor
	 * @param arg2 value of the second argument of type B defined for this Delayed
	 *             Batch Executor
	 * @param arg3 value of the third argument of type C defined for this Delayed
	 *             Batch Executor
	 * @param arg4 value of the fourth argument of type D defined for this Delayed
	 *             Batch Executor
	 * @return a <a href=
	 *         "https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>
	 *         for the result of type Z
	 *
	 *
	 */
	public Mono<Z> executeAsMono(A arg1, B arg2, C arg3, D arg4) {
		TupleMono<Z> tupleMono = new TupleMono<>(arg1, arg2, arg3, arg4);
		enlistTuple(tupleMono);
		Mono<Z> mono = tupleMono.getMono();
		return mono;
	}

	@Override
	protected List<Object> getResultListFromBatchCallBack(List<List<Object>> transposedTupleList) {
		List<Object> resultList = (List<Object>) batchCallBack.apply((List<A>) transposedTupleList.get(0),
				(List<B>) transposedTupleList.get(1), (List<C>) transposedTupleList.get(2),
				(List<D>) transposedTupleList.get(3));
		return resultList;
	}
}