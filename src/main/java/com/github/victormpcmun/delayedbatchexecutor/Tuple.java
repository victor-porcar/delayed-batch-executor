package com.github.victormpcmun.delayedbatchexecutor;

import java.util.Arrays;

abstract class Tuple<T> {
	protected T result;
	protected final Object[] argsAsArray;
	protected RuntimeException runtimeException;
	private int hashCode;

	Tuple(Object... argsAsArray) {
		super();
		this.result = null;
		this.argsAsArray = argsAsArray;
		this.hashCode = Arrays.hashCode(argsAsArray);

	}

	void copyResultAndRuntimeExceptionFromTuple(Tuple<T> tuple) {
		this.result = tuple.getResult();
		this.runtimeException = tuple.getRuntimeException();
	}

	public void setResult(T result) {
		this.result = result;
	}

	public void setRuntimeException(RuntimeException runtimeException) {
		this.runtimeException = runtimeException;
	}

	int getArgsSize() {
		return argsAsArray.length;
	}

	Object getArgumentByPosition(int argPosition) {
		return argsAsArray[argPosition];
	}

	public T getResult() {
		return result;
	}

	abstract void continueIfIsWaiting();

	RuntimeException getRuntimeException() {
		return runtimeException;
	}

	boolean hasRuntimeException() {
		return runtimeException != null;
	}

	@Override
	public boolean equals(Object o) {
		// o will never null
		Tuple<?> tuple = (Tuple<?>) o;
		return Arrays.equals(argsAsArray, tuple.argsAsArray);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(argsAsArray);
	}
}
