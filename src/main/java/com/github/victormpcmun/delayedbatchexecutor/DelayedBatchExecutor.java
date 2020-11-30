package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

abstract class DelayedBatchExecutor implements AutoCloseable {

	private static final String TO_STRING_FORMAT = "DelayedBatchExecutor {invocationsCounter=%d, callBackExecutionsCounter=%d, duration=%d, size=%d, bufferQueueSize=%d}";

	/**
	 * {@value com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor#MIN_TIME_WINDOW_TIME_IN_MILLISECONDS}
	 */
	public static final int MIN_TIME_WINDOW_TIME_IN_MILLISECONDS = 1;

	/**
	 * {@value com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor#MAX_TIME_WINDOW_TIME_IN_MILLISECONDS}
	 */
	public static final int MAX_TIME_WINDOW_TIME_IN_MILLISECONDS = 60 * 60 * 1000;

	/**
	 * {@value com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor#DEFAULT_FIXED_THREAD_POOL_COUNTER}
	 */
	public static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = 4;

	/**
	 * {@value com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor#DEFAULT_FIXED_THREAD_NAME_PREFIX}
	 */
	public static final String DEFAULT_FIXED_THREAD_NAME_PREFIX = "delayed-batch-executor-";

	/**
	 * {@value com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor#DEFAULT_BUFFER_QUEUE_SIZE}
	 */
	public static final int DEFAULT_BUFFER_QUEUE_SIZE = 8192;

	private final AtomicLong invocationsCounter;
	private final AtomicLong callBackExecutionsCounter;

	private Duration duration;
	private int maxSize;
	private ExecutorService executorService;
	private int bufferQueueSize;
	private UnicastProcessor<Tuple> source;
	private boolean removeDuplicates;

	private final boolean defaultExecutorServiceCreated;

	protected DelayedBatchExecutor(Duration duration, int maxSize, ExecutorService executorService, int bufferQueueSize,
			boolean removeDuplicates) {
		if (executorService == null) {
			executorService = getDefaultExecutorService();
			defaultExecutorServiceCreated = true;
		} else {
			defaultExecutorServiceCreated = false;
		}
		boolean configurationSuccessful = updateConfig(duration, maxSize, executorService, bufferQueueSize,
				removeDuplicates);
		if (!configurationSuccessful) {
			throw new RuntimeException("Illegal configuration parameters");
		}
		this.invocationsCounter = new AtomicLong(0);
		this.callBackExecutionsCounter = new AtomicLong(0);
		this.removeDuplicates = removeDuplicates;
	}

	/**
	 * Update the Duration and maxSize params of this Delayed Batch Executor,
	 * keeping the current existing value for executorService, bufferQueueSize and
	 * removeDuplicates <br>
	 * <br>
	 * This method is thread safe <br>
	 * 
	 * @param duration the new {@link Duration} for this Delayed Batch Executor
	 * @param maxSize  the new maxsize for this Delayed Batch Executor
	 * @return true if the configuration was successful updated, false otherwise
	 *
	 */
	public boolean updateConfig(Duration duration, int maxSize) {
		return updateConfig(duration, maxSize, executorService, bufferQueueSize, removeDuplicates);
	}

	/**
	 * Update the Duration, maxsize, ExecutorService, bufferQueueSize and
	 * removeDuplicates params of this Delayed Batch Executor <br>
	 * <br>
	 * This method is thread safe <br>
	 * 
	 * @param duration         the new {@link Duration} for this Delayed Batch
	 *                         Executor
	 * @param maxSize          the new maxsize for this Delayed Batch Executor
	 * @param executorService  the new {@link ExecutorService} for this Delayed
	 *                         Batch Executor
	 * @param bufferQueueSize  max size of the internal queue to buffer values
	 * @param removeDuplicates if true then duplicated arguments from execute*(...)
	 *                         methods are not passed to the batchCallBack
	 *                         (considering same {@link Object#hashCode()} and being
	 *                         {@link Object#equals(Object)})
	 * @return true if the configuration was successful updated, false otherwise
	 *
	 */
	public synchronized boolean updateConfig(Duration duration, int maxSize, ExecutorService executorService,
			int bufferQueueSize, boolean removeDuplicates) {
		boolean validateConfig = validateConfigurationParameters(duration, maxSize, executorService, bufferQueueSize);
		if (validateConfig) {
			boolean parameterAreEqualToCurrentOnes = parameterAreEqualToCurrentOnes(duration, maxSize, executorService,
					bufferQueueSize);
			if (!parameterAreEqualToCurrentOnes) {
				this.maxSize = maxSize;
				this.duration = duration;
				this.executorService = executorService;
				this.bufferQueueSize = bufferQueueSize;
				this.source = createBufferedTimeoutUnicastProcessor(duration, maxSize, bufferQueueSize,
						removeDuplicates);

			}
		}
		return validateConfig;
	}

	/**
	 * The count of invocations of all of the execute methods of this Delayed Batch
	 * Executor: execute(...), executeAsFuture(...) or executeAsMono(...) since the
	 * creation of this Delayed Batch Executor
	 * 
	 * @return the count of invocations of all blocking, Future and Mono invocations
	 *         since the creation of this Delayed Batch Executor
	 *
	 */
	public Long getInvocationsCounter() {
		return invocationsCounter.get();
	}

	/**
	 * The count of executions of the batchCallBack method since the creation of
	 * this Delayed Batch Executor
	 * 
	 * @return the count of executions of the batchCallBack method since the
	 *         creation of this Delayed Batch Executor
	 *
	 */
	public Long getCallBackExecutionsCounter() {
		return callBackExecutionsCounter.get();
	}

	/**
	 * The current {@link Duration} of this Delayed Batch Executor
	 * 
	 * @return the current {@link Duration} of this Delayed Batch Executor
	 *
	 */
	public Duration getDuration() {
		return duration;
	}

	/**
	 * The current size of this Delayed Batch Executor
	 * 
	 * @return the current size of this Delayed Batch Executor
	 *
	 */
	public Integer getMaxSize() {
		return maxSize;
	}

	/**
	 * The current max size of the internal queue to buffer values
	 * 
	 * @return the current max size of the internal queue to buffer values
	 *
	 */
	public Integer getBufferQueueSize() {
		return bufferQueueSize;
	}

	/**
	 * The current ExecutorService
	 * 
	 * @return the current ExecutorService
	 *
	 */
	public ExecutorService getExecutorService() {
		return executorService;
	}

	/**
	 * The removeDuplicates behaviour flag
	 * 
	 * @return the removeDuplicates behaviour flag
	 *
	 */

	public boolean isRemoveDuplicates() {
		return removeDuplicates;
	}

	/**
	 * static method that creates the default Executor Service, which is a
	 * {@link java.util.concurrent.Executors#newFixedThreadPool(int)} with the
	 * following number of threads given by constant
	 * {@link #DEFAULT_FIXED_THREAD_POOL_COUNTER}
	 * 
	 * @return the default Executor Service
	 */

	protected ExecutorService getDefaultExecutorService() {
		return getDefaultExecutorService(DEFAULT_FIXED_THREAD_POOL_COUNTER);
	}

	/**
	 * static method that creates the default Executor Service, which is a
	 * {@link java.util.concurrent.Executors#newFixedThreadPool(int)} with the given
	 * number of threads
	 * 
	 * @param threads number of threads of the FixedThreadPool
	 * @return the default Executor Service
	 */

	private ExecutorService getDefaultExecutorService(int threads) {
		return Executors.newFixedThreadPool(threads, new ThreadFactory() {
			private final AtomicInteger threadNumber = new AtomicInteger(1);

			@Override
			public Thread newThread(Runnable runnable) {
				return new Thread(runnable, DEFAULT_FIXED_THREAD_NAME_PREFIX + threadNumber.getAndIncrement());
			}
		});
	}

	@Override
	public void close() {
		if (defaultExecutorServiceCreated && !executorService.isShutdown()) {
			executorService.shutdown();
		}
	}

	@Override
	public String toString() {
		return String.format(TO_STRING_FORMAT, invocationsCounter.get(), callBackExecutionsCounter.get(),
				duration.toMillis(), maxSize, bufferQueueSize);
	}

	protected <Z> void enlistTuple(Tuple<Z> param) {
		invocationsCounter.incrementAndGet();
		source.onNext(param);
	}

	protected abstract List<Object> getResultListFromBatchCallBack(List<List<Object>> transposedTupleList);

	private void invokeBatchCallBackAndContinue(List<Tuple> tupleList) {
		List<Object> rawResultList = null;
		List<Object> resultFromCallBack;
		RuntimeException runtimeException = null;
		try {
			List<List<Object>> transposedTupleList = TupleListTransposer.transposeValuesAsListOfList(tupleList);
			rawResultList = getResultListFromBatchCallBack(transposedTupleList);
		} catch (RuntimeException re) {
			runtimeException = re;
		} finally {
			resultFromCallBack = resizeListFillingWithNullsIfNecessary(rawResultList, tupleList.size());
		}

		for (int indexTuple = 0; indexTuple < tupleList.size(); indexTuple++) {
			Tuple tuple = tupleList.get(indexTuple);
			tuple.setResult(resultFromCallBack.get(indexTuple));
			tuple.setRuntimeException(runtimeException);
			tuple.continueIfIsWaiting();
		}
	}

	private void assignValuesToDuplicatesAndContinue(TupleListDuplicatedFinder tupleListDuplicatedFinder) {
		Map<Integer, Integer> duplicatedMapIndex = tupleListDuplicatedFinder.getDuplicatedMapIndex();
		List<Tuple> allTupleList = tupleListDuplicatedFinder.getAllTupleList();
		for (Integer duplicatedIndex : duplicatedMapIndex.keySet()) {
			Tuple duplicatedTuple = allTupleList.get(duplicatedIndex);
			Tuple uniqueTuple = allTupleList.get(duplicatedMapIndex.get(duplicatedIndex));
			duplicatedTuple.copyResultAndRuntimeExceptionFromTuple(uniqueTuple);
			duplicatedTuple.continueIfIsWaiting();
		}
	}

	private void executeBatchCallBackRemovingDuplicates(List<Tuple> tupleList) {
		callBackExecutionsCounter.incrementAndGet();
		CompletableFuture.runAsync(() -> {
			TupleListDuplicatedFinder tupleListDuplicatedFinder = new TupleListDuplicatedFinder(tupleList);
			List<Tuple> tupleListUnique = tupleListDuplicatedFinder.getTupleListUnique();
			invokeBatchCallBackAndContinue(tupleListUnique);
			assignValuesToDuplicatesAndContinue(tupleListDuplicatedFinder);
		}, this.executorService);
	}

	private void executeBatchCallBackNotRemovingDuplicates(List<Tuple> tupleList) {
		callBackExecutionsCounter.incrementAndGet();
		CompletableFuture.runAsync(() -> {
			invokeBatchCallBackAndContinue(tupleList);
		}, this.executorService);
	}

	private UnicastProcessor<Tuple> createBufferedTimeoutUnicastProcessor(Duration duration, int maxSize,
			int bufferQueueSize, boolean removeDuplicates) {
		Queue<Tuple> blockingQueue = new ArrayBlockingQueue<>(bufferQueueSize); // =>
																				// https://github.com/reactor/reactor-core/issues/469#issuecomment-286040390
		UnicastProcessor<Tuple> newSource = UnicastProcessor.create(blockingQueue);
		if (removeDuplicates) {
			newSource.publish().autoConnect().bufferTimeout(maxSize, duration)
					.subscribe(this::executeBatchCallBackRemovingDuplicates);
		} else {
			newSource.publish().autoConnect().bufferTimeout(maxSize, duration)
					.subscribe(this::executeBatchCallBackNotRemovingDuplicates);
		}
		return newSource;
	}

	private boolean validateConfigurationParameters(Duration duration, int maxSize, ExecutorService executorService,
			int bufferQueueSize) {
		boolean sizeValidation = (maxSize >= 1);
		boolean durationValidation = duration != null && duration.toMillis() >= MIN_TIME_WINDOW_TIME_IN_MILLISECONDS
				&& duration.toMillis() <= MAX_TIME_WINDOW_TIME_IN_MILLISECONDS;
		boolean executorServiceValidation = (executorService != null);
		boolean bufferQueueSizeValidation = (bufferQueueSize >= 1);
		return sizeValidation && durationValidation && executorServiceValidation && bufferQueueSizeValidation;
	}

	private boolean parameterAreEqualToCurrentOnes(Duration duration, int size, ExecutorService executorService,
			int bufferQueueSize) {
		boolean sameDuration = this.duration != null && this.duration.compareTo(duration) == 0;
		boolean sameSize = (this.maxSize == size);
		boolean sameExecutorService = this.executorService != null && this.executorService == executorService; // same
																												// reference
																												// is
																												// enough
		boolean sameBufferQueueSize = (this.bufferQueueSize == bufferQueueSize);
		return sameDuration && sameSize && sameExecutorService && sameBufferQueueSize;
	}

	private List<Object> resizeListFillingWithNullsIfNecessary(List<Object> list, int desiredSize) {
		if (list == null) {
			list = Collections.nCopies(desiredSize, null);
		} else if (list.size() < desiredSize) {
			list = new ArrayList(list); // make it mutable in case it isn't
			list.addAll(Collections.nCopies(desiredSize - list.size(), null));
		}
		return list;
	}
}
