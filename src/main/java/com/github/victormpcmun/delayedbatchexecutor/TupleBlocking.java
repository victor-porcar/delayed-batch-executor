package com.github.victormpcmun.delayedbatchexecutor;

import java.util.concurrent.ExecutionException;

class TupleBlocking<T> extends TupleFuture<T> {

	TupleBlocking(Object... argsAsArray) {
		super(argsAsArray);
	}

	T getValueBlocking() {
		try {
			return get();
		} catch (ExecutionException e) {
			throw (RuntimeException) e.getCause();
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", e);
		}
	}
}
