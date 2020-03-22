package com.github.victormpcmun.delayedbatchexecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TupleListDuplicatedFinder {

    private List<Tuple> tupleListUnique;
    private Map<Integer,Integer> duplicatedMapIndex;

    public TupleListDuplicatedFinder(List<Tuple> tupleList) {
        Map<Integer,Integer> hashMap =new HashMap<>();

        duplicatedMapIndex = new HashMap<>();
        tupleListUnique = new ArrayList<>();
        for (int i=0; i<tupleList.size(); i++) {
            Tuple tuple = tupleList.get(i);

            int tupleHashCode = tuple.hashCode();
            Integer indexWithHashMap = hashMap.get(tupleHashCode);

            if (indexWithHashMap!=null)  {
                if (tuple.equals(tupleList.get(indexWithHashMap))) {
                    duplicatedMapIndex.put(i,hashMap.get(tupleHashCode));
                } else {
                    tupleListUnique.add(tuple);
                }

            } else {
                hashMap.put(tupleHashCode,i);
                tupleListUnique.add(tuple);
            }
        }
    }

    List<Tuple> getTupleListUnique() {
        return tupleListUnique;
    }
    Map<Integer,Integer> getDuplicatedMapIndex() {
        return duplicatedMapIndex;
    }
}
