package com.github.victormpcmun.delayedbatchexecutor;


import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

class TupleMono<T> extends Tuple<T>  {

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
              doSink();
            }

        }


        public void setMonoSink(MonoSink<T> monoSink) {
            this.monoSink = monoSink;
            if (isDone()) {
                doSink();
            }
        }

        public boolean existSink() {
            return monoSink!=null;
        }


        private void doSink() {
            if (hasRuntimeException()) {
                monoSink.error(getRuntimeException());
            }
            monoSink.success(result);
        }
}
