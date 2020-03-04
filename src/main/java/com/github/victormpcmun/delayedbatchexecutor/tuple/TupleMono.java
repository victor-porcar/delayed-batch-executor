package com.github.victormpcmun.delayedbatchexecutor.tuple;


import reactor.core.publisher.MonoSink;

public class TupleMono<T> extends Tuple<T>  {


    MonoSink<T> monoSink;

    public TupleMono(Object... argsAsArray) {
        super(argsAsArray);
    }

    @Override
   public void continueIfIsWaiting() {
        if (existSink()  && isDone()) {
            monoSink.success(result);
        }

    }


    public void setMonoSink(MonoSink<T> monoSink) {
        this.monoSink = monoSink;
        if (isDone()) {
            monoSink.success(result);
        }
    }

    public boolean existSink() {
        return monoSink!=null;
    }
}
