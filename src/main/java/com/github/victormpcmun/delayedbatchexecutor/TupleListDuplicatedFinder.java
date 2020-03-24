package com.github.victormpcmun.delayedbatchexecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TupleListDuplicatedFinder {

    private List<Tuple> allTupleList;
    private List<Tuple> tupleListUnique;
    private Map<Integer,Integer> duplicatedMapIndex;
    Map<Integer,List<Integer>> hashCodeByTuplesIndexInList =new HashMap<>();

    TupleListDuplicatedFinder(List<Tuple> allTupleList) {
        this.allTupleList = allTupleList;
        duplicatedMapIndex = new HashMap<>();
        tupleListUnique = new ArrayList<>();

        for (int i = 0; i< allTupleList.size(); i++) {
            Tuple tuple = allTupleList.get(i);
            int tupleHashCode = tuple.hashCode();
            List<Integer> listOfIndexesMatchingHashCode = hashCodeByTuplesIndexInList.get(tupleHashCode);

            if (listOfIndexesMatchingHashCode==null)  {
                List<Integer> listOfIndexes = new ArrayList<>();
                listOfIndexes.add(i);
                hashCodeByTuplesIndexInList.put(tupleHashCode, listOfIndexes);
                tupleListUnique.add(tuple);
            } else {
                Integer matchingIndex = listOfIndexesMatchingHashCode.stream().filter(index -> tuple.equals(allTupleList.get(index))).findAny().orElse(null);
                if (matchingIndex!=null) {
                    duplicatedMapIndex.put(i,matchingIndex);
                } else {
                    listOfIndexesMatchingHashCode.add(i);
                    tupleListUnique.add(tuple);
                }
            }

        }
    }




    List<Tuple> getAllTupleList() {
        return allTupleList;
    }

    List<Tuple> getTupleListUnique() {
        return tupleListUnique;
    }
    Map<Integer,Integer> getDuplicatedMapIndex() {
        return duplicatedMapIndex;
    }
}
