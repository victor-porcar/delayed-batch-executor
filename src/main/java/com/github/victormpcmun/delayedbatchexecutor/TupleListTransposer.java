package com.github.victormpcmun.delayedbatchexecutor;

import java.util.ArrayList;
import java.util.List;

class TupleListTransposer {

	static List<List<Object>> transposeValuesAsListOfList(List<Tuple> paramList) {
		int argsCounter = paramList.get(0).getArgsSize();
		List<List<Object>> listOfListsOfArgs = buildEmptyListOfListsOfArgs(argsCounter);
		for (Tuple tuple : paramList) {
			for (int argPosition = 0; argPosition < argsCounter; argPosition++) {
				Object object = tuple.getArgumentByPosition(argPosition);
				listOfListsOfArgs.get(argPosition).add(object);
			}
		}
		return listOfListsOfArgs;
	}

	private static List<List<Object>> buildEmptyListOfListsOfArgs(int argsCounter) {
		List<List<Object>> listOfListsOfArgs = new ArrayList<>();

		for (int argPosition = 0; argPosition < argsCounter; argPosition++) {
			listOfListsOfArgs.add(argPosition, new ArrayList<>());
		}
		return listOfListsOfArgs;
	}
}
