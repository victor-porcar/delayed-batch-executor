package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

class TupleMono<T> extends Tuple<T> {

    private static final Integer MILLIS_TO_SLEEP_PER_ITERATION_TO_WAIT_SET_SINK = 10;
    private static final Integer ITERATIONS_TO_WAIT_SET_SINK = 20;
    MonoSink<T> monoSink;
    Mono<T> mono;

    public static <T> TupleMono<T> create(Object... argsAsArray) {
        TupleMono<T> tupleMono = new TupleMono<T>(argsAsArray);
        tupleMono.mono = Mono.create(monoSink -> tupleMono.monoSink = monoSink);
        return tupleMono;
    }

    public Mono<T> getMono() {
        return mono;
    }

    private TupleMono(Object... argsAsArray) {
        super(argsAsArray);
    }

    @Override
    public void continueIfIsWaiting() {
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


