package com.github.victormpcmun.delayedbatchexecutor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TupleListTransposerTest {

	private static final String ARG11 = "ARG11";
	private static final String ARG12 = "ARG11";
	private static final String ARG21 = "ARG11";
	private static final String ARG22 = "ARG11";

	@Test
	public void tupleListTest() {
		List<Tuple> tupleList = new ArrayList<>();
		Tuple<Integer> tuple1 = new TupleFuture<>(ARG11, ARG12);
		Tuple<Integer> tuple2 = new TupleFuture<>(ARG21, ARG22);

		tupleList.add(tuple1);
		tupleList.add(tuple2);

		List<List<Object>> tranposedTupleAsListOfList = TupleListTransposer.transposeValuesAsListOfList(tupleList);

		Assert.assertEquals(tranposedTupleAsListOfList.get(0).get(0), ARG11);
		Assert.assertEquals(tranposedTupleAsListOfList.get(0).get(0), ARG21);
		Assert.assertEquals(tranposedTupleAsListOfList.get(0).get(0), ARG12);
		Assert.assertEquals(tranposedTupleAsListOfList.get(0).get(0), ARG22);
	}

}
