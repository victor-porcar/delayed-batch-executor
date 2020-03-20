
 ## Introduction
 
 I described in my post [Optimizing Data Repositories Usage in Java Multi-Threaded Applications](https://dzone.com/articles/optimizing-data-repositories-usage-in-java-multith) a simple mechanism called DelayedBatchExecutor  to decrease the number of required queries in multithreaded applications by batching them. 
 
 While this mechanism works well, it does need to block the threads for a interval of time, which is not optimal in some cases. I have released a new version of DelayedBatchExecutor that includes non-blocking behaviour in two ways:

- using [Futures](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/Future.html)
- using Reactive programming (Reactor framework)

## Rationale behind of DelayeBatchExecutor

There are several scenarios in which concurrent threads execute the same query (with different parameter) to a database at almost the same time. 

For example, a REST endpoint serving tens or hundreds requests per second in which each one requires to retrieve a row from table by a different Id.

In a similar way, another typical scenario is a message listener that consumes many messages per second and requires to execute a query by a different Id to process each one.

In these cases, if the number of concurrent threads is high, the database executes many times the same query in a short interval of time (say few milliseconds) like these:
```sql
SELECT * FROM TABLE WHERE ID   = <Id1>
SELECT * FROM TABLE WHERE ID   = <Id2>
...
SELECT * FROM TABLE WHERE ID   = <Idn>
```
DelayedBatchExecutor is a component that allows easily to *convert* these multiple executions of one query with one parameter to just one single query with n parameters, like this one:

```sql
SELECT * FROM TABLE WHERE ID IN (<id1>, <id2>, ..., <idn>)
```

The advantages of executing one query with n parameters instead of n queries of 1 argument are the following:

* The usage of network resources is reduced dramatically: The number of round-trips to the database is 1 instead of n.

* Database optimization: you would be surprised how well databases optimize queries of n parameters. Pick any large table of your schema and analyse the execution time, execution plan and resources usage for a single query of n parameters versus n queries of 1 parameter.

* The usage of database connections from the application pool is reduced: there are more available connections overall, which means less waiting time for a connection on peak times.

In short, it is much more efficient executing 1 query of n parameters than n queries of one parameter, which means that the system as a whole requires less resources.

## DelayedBatchExecutor In Action

It basically works by creating time windows where the parameters of the queries executed during the time window are collected in a list. 
As soon as the time window finishes, the list is passed (via callback) to a method that executes one query with all the parameters in the list and returns another list with the results. Each thread receives their corresponding result from the result list according to one of the following policies as explained below: blocking , non-blocking (Future), non-blocking (Reactive).

The mechanism for managing the time windows and the list of parameters is developed using a [bufferedTimeout Flux](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#bufferTimeout-int-java.time.Duration-) of the [Reactor Framework] (https://projectreactor.io/)  

A DelayedBatchExecutor is defined by three parameters:
 
 * TimeWindow: defined as java.time.Duration
 * max size: it is the max number of items to be collected in the list
 * batchCallback: it receives the parameters list to perform a single query and returns a list with the corresponding results. 
    - It can be implemented as method reference or lambda expression.
    - It is invoked automatically as soon as the TimeWindow is finished OR the collection list is full. 
    - The returned list must have a correspondence in elements with the parameters list, this means that the value of position 0 of the returned list must be the one corresponding to parameter in position 0 of the param list and so on...
	
  Let's define a DelayedBatchExecutor to receive an Integer value as parameter and return a String, and having a time window = 50 milliseconds, a max size = 20 elements and having the batchCallback defined as method reference: 
  
  ```java
DelayedBatchExecutor2<String,Integer> dbe = DelayedBatchExecutor2.create(Duration.ofMillis(50), 20, this::myBatchCallBack);
  
...
  
List<String> myBatchCallBack(List<Integer> listOfIntegers) {
	List<String>  resultList = ...// execute query:SELECT * FROM TABKE WHERE ID IN (listOfIntegers.get(0), ..., listOfIntegers.get(n));
                                // using your favourite API: JDBC, JPA, Hibernate.
  	...
  	return resultList;
}
```

The same DelayedBatchExecutor2 but having the callback defined as lambda expression would be:

```java
DelayedBatchExecutor2<String,Integer> dbe = DelayedBatchExecutor2.create(Duration.ofMillis(50), 20, listOfIntegers-> 
{
  List<String>  resultList = ...// execute query:SELECT * FROM TABLE WHERE ID IN (listOfIntegers.get(0), ..., listOfIntegers.get(n));
                                // using your favourite API: JDBC, JPA, Hibernate
  ...
  return resultList;
  
 });
  ``` 

Once defined, it is very easy to use from the code executed in each thread

  ```java
// this code is executed in one of the multiple threads
int param=...;
String result = dbe.execute(param); // all the threads executing this code within a interval of 50 ms will have 
			            // their parameters (an integer value) collected in a list (list of integers) 
				    // that will be passed to the callback method, and from the list returned from this
				    // method, each thread will receive its corresponding value
				    // all this managed behind the scenes by the DelayedBatchExecutor
}
```
NOTE:
- To create DelayedBatchExecutor for more than one parameter see Foot Note 1
- In the example below, the thread is stopped when the execute(...) method is executed until the result is available (blocking behaviour). This is one of the three execution policies of the DelayedBatchExecutor


### Execution Policies

There are three policies to use a DelayedBatchExecutor from the code being executed from the threads

#### Blocking

The thread is blocked until the result is available, it is implemented by using the method `execute(...)`
 
```java 
    int param = ...
	...
    String result = dbe.execute(param); // this thread will be blocked until the result is available
    // compute with result
```
The following diagram depicts how blocking policy works:

![Blocking image](/src/main/javadoc/doc-files/blocking.svg)


#### Non-blocking (java.util.concurrent.Future)

The thread is not blocked, it is implemented by using the method `executeAsFuture(...)`

```java 
    int param = ...
       ...	
    Future<String> resultFuture = dbe.executeAsFuture(param); // the thread will not  be blocked
    // compute something else
    String result = resultFuture.get();  // Blocks the thread until the result is available (if necessary)
    // compute with result
```

The following diagram depicts how Future policy works:

![Future image](/src/main/javadoc/doc-files/future.svg)


#### Non-blocking (Reactive using Reactor framework):
 
 The thread is not blocked, it is implemented by using the method `executeAsMono(...)`
 
```java 
    int param1 =...
       ...
    reactor.core.publisher.Mono<String> resultMono = dbe.executeAsMono(param1); // the thread will not  be blocked
    // compute something else
    resultMono.subscribe(stringResult -> {
         // compute with stringResult
      }
```
The following diagram depicts how Reactive policy works:

![Reactive image](/src/main/javadoc/doc-files/mono.svg)

### Advanced Usage

There are two parameters of a DelayedBatchExecutor that must be known to get the most of it:

- ExecutorService: the callback method is actually executed in a parallel thread, which is provided by an java.util.concurrent.ExecutorService. By default this Executor is `Executors.newFixedThreadPool(4)`.
 NOTE: the execution of the callback of a DelayedBatchExecutor does not prevent it to open a new time window if required as long as there are threads availables from the ExecutorService.

- bufferQueueSize: it is the max size of the internal list, by default its value is 8192

These parameters can be set by using the following constructor:

```java
... 
int maxSize=20;
ExecutorService executorService= Executors.newFixedThreadPool(10);
int bufferQueueSize= 16384;
  
DelayedBatchExecutor2<Integer,String> dbe = DelayedBatchExecutor2.create(
    Duration.ofMillis(200), 
    maxSize,
    executorService,
    bufferQueueSize,
    this::myBatchCallBack);
```
 At any time, the configuration paramaters can be updated by using this thread safe method
 
```java
...
dbe.updateConfig(
    Duration.ofMillis(200), 
    maxSize,
    executorService,
    bufferQueueSize,
    this::myBatchCallBack);
 ```

-----
Foot Note 1:  The example shows a DelayedBatchExecutor for a parameter of type Integer and a return type of String, hence DelayedBatchExecutor2<String,Integer>

For a DelayedBatchExecutor for two parameters (say Integer and Date) and a returning type String, the definition would be:
DelayedBatchExecutor3<String,Integer,Date> and so on...
