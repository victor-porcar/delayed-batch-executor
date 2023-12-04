package com.github.victormpcmun.delayedbatchexecutor.simulator;

import com.github.victormpcmun.delayedbatchexecutor.DelayedBatchExecutor3;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;

public class DBEMultipleRequestsTest {

	@Test
	public void manyRequestsPerSecondTest() {
		// tests parameters
		int requestsPerSecond = 200;
		int testDurationSeconds = 10;
		// delayed-batch-executor
		long dbeDurationWindowInMillis=200;
		int dbeMaxSize = 200;
		int dbeCallBackToCompleteInMillis=50;

		launchTest(requestsPerSecond,testDurationSeconds,dbeDurationWindowInMillis,dbeMaxSize,dbeCallBackToCompleteInMillis);

	}



	private void launchTest(
			int requestsPerSecond,
			int testDurationInSeconds,
			long dbeDurationWindowInMillis,
			int dbeMaxSize,
			int msToCompleteDbeCallBack) {


		//-----------

		DelayedBatchExecutor3<String, Long,Integer> dbe3 = DBE3Builder.buildDBE(dbeDurationWindowInMillis, dbeMaxSize, msToCompleteDbeCallBack);

		BiFunction<Long,Integer,String> biFunction = (duration,maxSize) -> dbe3.execute(duration,maxSize);

		ClientParallelRequestSimulator clientParallelRequestSimulator =  new ClientParallelRequestSimulator(
				requestsPerSecond,
				testDurationInSeconds,
				biFunction
		);

		clientParallelRequestSimulator.go();

		// wait two seconds to allow threads to finish
		SimulatorUtils.sleepCurrentThread((testDurationInSeconds+2)*1000);

		Assert.assertEquals(
				clientParallelRequestSimulator.getTotalExpectedRequests(),
				clientParallelRequestSimulator.getTotalRequestsSinceBeginning());

		clientParallelRequestSimulator.logExecutionReport();
	}




}
