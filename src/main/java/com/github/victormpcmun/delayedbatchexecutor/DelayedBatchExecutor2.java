package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Delayed Batch Executor for one argument of type A and return type Z <br>
 * 
 * <pre>
 * {@code
 * DelayedBatchExecutor2<String,Integer> dbe = DelayedBatchExecutor2.create(Duration.ofMillis(50), 10, this::myBatchCallback);
 *
 * ...
 *
 * public void usingDelayedBatchExecutor(Integer param1) {
 *
 *    // using blocking behaviour
 *    String stringResult1 = dbe.execute(param1); // the thread will be blocked until the result is available
 *    // compute with stringResult1
 *
 *
 *    // using Future
 *    Future<String> resultAsFutureString = dbe.executeAsFuture(param1); // the thread will not  be blocked
 *    // compute something else
 *    String stringResult2 = resultAsFutureString.get();  // Blocks the thread if necessary for the computation to complete, and then retrieves its result.
 *    // compute with stringResult2
 *
 *
 *    // using Mono
 *    Mono<String> monoResult = dbe.executeAsMono(param1); // the thread will not  be blocked
 *    // compute something else
 *    monoResult.subscribe(stringResult3 -> {
 *     // compute with stringResult
 *    });
 *
 * }
 *
 * ...
 *
 * List<String> myBatchCallback(List<Integer> arg1List) {
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

public class DelayedBatchExecutor2<Z, A> extends DelayedBatchExecutor {

	/**
	 * Receive as argument a List of type A and returns a List of type Z. It can be
	 * implemented as a lambda expression or method reference <br>
	 * <br>
	 * 
	 * <pre>
	 * <b>Lambda expression</b>
	 * {@code
	 * DelayedBatchExecutor2<String,Integer> dbe = DelayedBatchExecutor2.create(Duration.ofMillis(50), 10, arg1List ->
	 * {
	 *      //arg1List is a List<Integer>
	 *      List<String> result = ...
	 *	    ...
	 *      return result;
	 *});
	 *}
	 * <b>Method reference</b>
	 * {@code
	 * DelayedBatchExecutor2<Integer,String> dbe = DelayedBatchExecutor2.create(Duration.ofMillis(50), 10, this::myBatchCallBack);
	 * ...
	 * List<String> myBatchCallBack(List<Integer> arg1List) {
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
	public interface BatchCallBack2<Z, A> {
		List<Z> apply(List<A> firstParam);
	}

	private final BatchCallBack2<Z, A> batchCallBack;

	/**
	 * Factory method to create an instance of a Delayed Batch Executor for one
	 * argument of type A and return type Z. Similar to
	 * {@link DelayedBatchExecutor2#create(Duration, int, ExecutorService, int, boolean, BatchCallBack2)}
	 * defaulting to: <br>
	 * <br>
	 * -executorService: the one returned by static method
	 * {@link #getDefaultExecutorService()} <br>
	 * -bufferQueueSize: the value of constant {@link #DEFAULT_BUFFER_QUEUE_SIZE}
	 * <br>
	 * -removeDuplicates:true <br>
	 * 
	 * @param <Z>            the return type
	 * @param <A>            the type of the argument
	 * @param duration       the time window, defined as {@link Duration }.
	 * @param size           the max collected size. As soon as the count of
	 *                       collected parameters reaches this size, the
	 *                       batchCallBack method is executed
	 * @param batchCallback2 the method reference or lambda expression that receives
	 *                       a list of type A and returns a list of Type Z (see
	 *                       {@link BatchCallBack2})
	 * @return an instance of {@link DelayedBatchExecutor2}
	 *
	 */

	public static <Z, A> DelayedBatchExecutor2<Z, A> create(Duration duration, int size,
			BatchCallBack2<Z, A> batchCallback2) {
		return new DelayedBatchExecutor2<>(duration, size, null, DEFAULT_BUFFER_QUEUE_SIZE, true, batchCallback2);
	}

	/**
	 * Factory method to create an instance of a Delayed Batch Executor for one
	 * argument of type A and return type Z <br>
	 * 
	 * @param <Z>              the return type
	 * @param <A>              the type of the argument
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
	 * @param batchCallback2   the method reference or lambda expression that
	 *                         receives a list of type A and returns a list of Type
	 *                         Z (see {@link BatchCallBack2})
	 * @return an instance of {@link DelayedBatchExecutor2}
	 *
	 */

	public static <Z, A> DelayedBatchExecutor2<Z, A> create(Duration duration, int size,
			ExecutorService executorService, int bufferQueueSize, boolean removeDuplicates,
			BatchCallBack2<Z, A> batchCallback2) {
		return new DelayedBatchExecutor2<>(duration, size, executorService, bufferQueueSize, removeDuplicates,
				batchCallback2);
	}

	private DelayedBatchExecutor2(Duration duration, int size, ExecutorService executorService, int bufferQueueSize,
			boolean removeDuplicates, BatchCallBack2<Z, A> batchCallBack) {
		super(duration, size, executorService, bufferQueueSize, removeDuplicates);
		this.batchCallBack = batchCallBack;
	}

	/**
	 * Return the result of type Z (blocking the thread until the result is
	 * available), which is obtained from the returned list of the batchCallBack
	 * method for the given argument <br>
	 * <br>
	 * It will throw any {@link RuntimeException} thrown inside of the
	 * {@link BatchCallBack2 } <br>
	 * It will throw a {@link RuntimeException} if the internal buffer Queue of this
	 * Delayed Batch Executor is full. <br>
	 * <br>
	 * <img src="{@docRoot}/doc-files/blocking.svg" alt="blocking">
	 * 
	 * @param arg1 value of the argument of type A defined for this Delayed Batch
	 *             Executor
	 * @return the result of type Z
	 *
	 *
	 */
	public Z execute(A arg1) {
		TupleBlocking<Z> tupleBlocking = new TupleBlocking<>(arg1);
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
	 * If a {@link RuntimeException} is thrown inside of the {@link BatchCallBack2
	 * }, then it will be the cause of the checked Exception
	 * {@link ExecutionException} thrown by {@link Future#get()} as per contract of
	 * {@link Future#get()} <br>
	 * <br>
	 * 
	 * @param arg1 value of the argument of type A defined for this Delayed Batch
	 *             Executor
	 * @return a {@link Future } for the result of type Z
	 *
	 */
	public Future<Z> executeAsFuture(A arg1) {
		TupleFuture<Z> tupleFuture = new TupleFuture<>(arg1);
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
	 * If a {@link RuntimeException} is thrown inside of the {@link BatchCallBack2
	 * }, then it will be the propagated as any {@link RuntimeException } thrown
	 * from <a href=
	 * "https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>
	 * <br>
	 * <br>
	 * 
	 * @param arg1 value of the argument of type A defined for this Delayed Batch
	 *             Executor
	 * @return a <a href=
	 *         "https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono</a>
	 *         for the result of type Z
	 *
	 *
	 */
	public Mono<Z> executeAsMono(A arg1) {
		TupleMono<Z> tupleMono = new TupleMono<>(arg1);
		enlistTuple(tupleMono);
		Mono<Z> mono = tupleMono.getMono();
		return mono;
	}

	@Override
	protected List<Object> getResultListFromBatchCallBack(List<List<Object>> transposedTupleList) {
		List<Object> resultList = (List<Object>) batchCallBack.apply((List<A>) transposedTupleList.get(0));
		return resultList;
	}
}