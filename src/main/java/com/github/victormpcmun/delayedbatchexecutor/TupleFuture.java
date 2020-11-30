package com.github.victormpcmun.delayedbatchexecutor;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TupleFuture<T> extends Tuple<T> implements Future<T> {

	private final Instant initInstant;
	private Instant endInstant;
	private boolean done;

	TupleFuture(Object... argsAsArray) {
		super(argsAsArray);
		this.done = false;
		this.initInstant = Instant.now();
	}

	Future<T> getFuture() {
		return this;
	}

	@Override
	void continueIfIsWaiting() {
		synchronized (this) {
			this.endInstant = Instant.now();
			this.done = true;
			this.notify();
		}
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		try {
			return get(0L);
		} catch (TimeoutException te) {
			throw new RuntimeException("This RuntimeException should never thrown at this point.", te);
		}
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		long milliseconds = TimeUnit.MILLISECONDS.convert(timeout, unit);
		return get(milliseconds);
	}

	public Duration getDelayedTime() {
		return Duration.between(initInstant, endInstant);
	}

	private T get(long millisecondsWait) throws InterruptedException, ExecutionException, TimeoutException {
		synchronized (this) {
			if (!done) {
				if (millisecondsWait == 0L) {
					this.wait();
				} else {
					this.wait(millisecondsWait);
					if (!done) {
						throw new TimeoutException("can not get the result in " + millisecondsWait);
					}
				}
			}
		}
		if (hasRuntimeException()) {
			throw new ExecutionException(getRuntimeException());
		}
		return result;
	}
}
