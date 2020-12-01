package com.github.victormpcmun.delayedbatchexecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TupleListDuplicatedFinder<T> {

	private List<Tuple<T>> allTupleList;
	private List<Tuple<T>> tupleListUnique;
	private Map<Integer, Integer> duplicatedMapIndex;
	Map<Integer, List<Integer>> hashCodeByTuplesIndexInList = new HashMap<>();

	TupleListDuplicatedFinder(List<Tuple<T>> allTupleList) {
		this.allTupleList = allTupleList;
		duplicatedMapIndex = new HashMap<>();
		tupleListUnique = new ArrayList<>();

		for (int index = 0; index < allTupleList.size(); index++) {
			Tuple<T> tuple = allTupleList.get(index);
			int tupleHashCode = tuple.hashCode();
			List<Integer> listOfIndexesMatchingHashCode = hashCodeByTuplesIndexInList.get(tupleHashCode);

			if (listOfIndexesMatchingHashCode == null) {
				List<Integer> listOfIndexes = new ArrayList<>();
				listOfIndexes.add(index);
				hashCodeByTuplesIndexInList.put(tupleHashCode, listOfIndexes);
				tupleListUnique.add(tuple);
			} else {
				Integer matchingIndex = listOfIndexesMatchingHashCode.stream()
						.filter(indexInList -> tuple.equals(allTupleList.get(indexInList))).findAny().orElse(null);
				if (matchingIndex != null) {
					duplicatedMapIndex.put(index, matchingIndex);
				} else {
					listOfIndexesMatchingHashCode.add(index);
					tupleListUnique.add(tuple);
				}
			}

		}
	}

	List<Tuple<T>> getAllTupleList() {
		return allTupleList;
	}

	List<Tuple<T>> getTupleListUnique() {
		return tupleListUnique;
	}

	Map<Integer, Integer> getDuplicatedMapIndex() {
		return duplicatedMapIndex;
	}
}
