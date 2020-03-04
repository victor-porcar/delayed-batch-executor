package com.github.victormpcmun.delayedbatchexecutor.tuple;

import java.util.ArrayList;
import java.util.List;

public class TupleListArgs {

    private final List<List<Object>> listOfListsOfArgs;

    public TupleListArgs(List<Tuple> paramList) {
        int argsCounter=paramList.get(0).getArgsSize();
        listOfListsOfArgs = buildEmptyListOfListsOfArgs(argsCounter);
        for (Tuple tuple:paramList) {
            for (int argPosition = 0; argPosition < argsCounter; argPosition++) {
                Object object = tuple.getArgumentByPosition(argPosition);
                listOfListsOfArgs.get(argPosition).add(object);
            }
        }
    }

    public <T> List<T> getArgsList(int argNumber) {
        return (List<T>) listOfListsOfArgs.get(argNumber);
    }

    private List<List<Object>> buildEmptyListOfListsOfArgs(int argsCounter) {
        List<List<Object>> listOfListsOfArgs = new ArrayList<>();

        for (int argPosition=0; argPosition<argsCounter; argPosition++) {
            listOfListsOfArgs.add(argPosition, new ArrayList<>());
        }
        return listOfListsOfArgs;
    }

}
