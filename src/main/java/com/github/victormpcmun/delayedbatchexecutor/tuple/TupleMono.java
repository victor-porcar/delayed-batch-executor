package com.github.victormpcmun.delayedbatchexecutor.tuple;


import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class TupleMono<T> extends Tuple<T>  {

      MonoSink<T> monoSink;
      Mono<T> mono;

      public static <T> TupleMono<T>  create(Object... argsAsArray) {
        TupleMono tupleMono = new TupleMono(argsAsArray);
        tupleMono.mono = Mono.create(monoSink -> tupleMono.monoSink=monoSink);
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
