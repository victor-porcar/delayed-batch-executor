package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

class TupleMono<T> extends Tuple<T> {

    private static final Integer MILLIS_TO_SLEEP_PER_ITERATION_TO_WAIT_SET_SINK = 10;
    private static final Integer ITERATIONS_TO_WAIT_SET_SINK = 20;
    private MonoSink<T> monoSink;
    private Mono<T> mono;

    TupleMono(Object... argsAsArray) {
        super(argsAsArray);
        this.mono = Mono.create(monoSink -> this.monoSink = monoSink);
    }

    Mono<T> getMono() {
        return mono;
    }

    @Override
    void continueIfIsWaiting() {
        whileUntilSinkIsSet();
        if (hasRuntimeException()) {
            monoSink.error(getRuntimeException());
        } else {
            monoSink.success(result);
        }
    }

    private void whileUntilSinkIsSet() {
        int counterIteration = ITERATIONS_TO_WAIT_SET_SINK;
        while (monoSink == null && counterIteration > 0) {
            try {
                Thread.sleep(MILLIS_TO_SLEEP_PER_ITERATION_TO_WAIT_SET_SINK);
            } catch (InterruptedException e) {
                throw new RuntimeException("Can not wait until sink is set because InterruptedException", e);
            }
            counterIteration--;
        }
        if (monoSink == null) {
            throw new RuntimeException("MonoSink can not be set");
        }
    }
}


