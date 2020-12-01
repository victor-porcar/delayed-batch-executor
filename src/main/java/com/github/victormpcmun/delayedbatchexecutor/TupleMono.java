package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.Mono;

class TupleMono<T> extends Tuple<T> {

	private Mono<T> mono;
	private boolean done;

	private Object locker = new Object();

	TupleMono(Object... argsAsArray) {
		super(argsAsArray);
		this.done = false;
		this.mono = Mono.create(monoSink -> {
			if (!done) {
				try {
					// TODO: avoid this wait, any good suggestion of how to do it ?
					synchronized (locker) {
						locker.wait();
					}
				} catch (InterruptedException e) {
					throw new RuntimeException("Can not get Value from thread");
				}
			}
			if (hasRuntimeException()) {
				monoSink.error(getRuntimeException());
			} else {
				monoSink.success(result);
			}
		});
	}

	Mono<T> getMono() {
		return mono;
	}

	@Override
	void continueIfIsWaiting() {
		synchronized (locker) {
			this.done = true;
			locker.notify();
		}
	}
}
