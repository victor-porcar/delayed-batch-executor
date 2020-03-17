
 ## Introduction
 
 I described a simple mechanism called DelayedBatchExecutor in my post [Optimizing Data Repositories Usage in Java Multi-Threaded Applications](https://dzone.com/articles/optimizing-data-repositories-usage-in-java-multith) to decrease the number of required queries in multithreaded applications by batching them. 
 
 While this mechanism works well, it does block the invoking thread for a interval of time, which is not optimal in some cases. I have released a new version of DelayedBatchExecutor that includes non-blocking behaviour in two ways:

- using Futures (java.util.concurrent.Future) 
- using Reactive programming (Reactor framework)

## Rationale behind DelayeBatchExecutor

There are several scenarios in which concurrent threads execute the same query (with different parameter) to a database at almost the same time. 

For example, a REST endpoint that is massively hit (tens or hundreds hits per second) in which it is required to execute a query to retrieve an entity by a different resourceId. 

Another typical scenario is a  messaging listener that consumes tens or hundreds of messages per second and requires for each one to retrieve a row from a table by a different id when processing them.

In all these cases, if the number of concurrent threads is high, the database executes many times the same query in a short interval of time
```sql
SELECT * FROM TABLE WHERE ID   = <Id1>
SELECT * FROM TABLE WHERE ID   = <Id2>
...
SELECT * FROM TABLE WHERE ID   = <Idn>
```
As pointed in my  [Optimizing Data Repositories Usage in Java Multi-Threaded Applications](https://dzone.com/articles/optimizing-data-repositories-usage-in-java-multith) , DelayedBatchExecutor is a component that allows to *convert* easily these multiple executions of one query with one parameter to just one single query with n parameters, like this one:

```sql
SELECT * FROM TABLE WHERE ID IN (<id1>, <id2>, ..., <idn>)
```

The advantages of executing one query with n parameters instead of n queries of 1 argument are the following:

* The usage of network resources is reduced dramatically: The number of round-trips to the database is 1 instead of n.

* Database optimizer: you would be surprised how well databases optimize queries of n parameters. Pick any table of your schema and analyse the execution time, execution plan and resources usage for a single query of n parameters versus n queries of 1 parameter.

* The usage of database connections from the application pool is reduced: there are more available connections overall, which means less waiting time for a connection in peak times.

In short, it is much more efficient executing 1 query of n parameters than n queries of one parameter.

## DelayedBatchExecutor in detail

It basically works by creating  window times where the indivual parameters of the queries are collected in a list. 
As soon as the window time finishes, the list is passed (via callback)  to a  method that implemenets logic to execute one query with  all the parameters in the list and returns a list with the results. Each thread receives their corresponding result following one of the three available policies detailed below: blocking , non-blocking (Future), non-blocking (Reactive).

A DelayedBatchExecutor is defined by three parameters:
 
 * WindowTime: defined as java.time.Duration
 * max size: it is the max number of items to be collected in the list
 * batchCallback: it receives the parameters list to perform a single query and returns a list with the corresponding results. 
    - It can be implemented as a lambda expression or method reference.
    - It is invoked automatically as soon as the WindowTime is finished OR the collection list is full. 
    - The returned listed must have a correspondence in elements with the parameters list, this means that the value of position 0 of the returned list must be the one corresponding with parameter in position 0 of the param list and so on...).
    - The callBack is actually executed in a Thead provided by a ExecutorService (see Advanced Usage below), which means that the execution of this callback method does not necessarily block the DelayedBatchExecutor.
	
  Let's define a DelayedBatchExecutor(see footnote 1)  to receive as parameter a Integer and return a String, and having a window time = 200 milliseconds and a max size = 20 elements 
  
  #### Using a Lambda
```java
DelayedBatchExecutor2<String,Integer> dbe = DelayedBatchExecutor2.create(Duration.ofMillis(200), 20, listOfIntegers-> 
{
  List<String>  resultList = ...// execute query:SELECT * FROM TABLE WHERE ID IN (listOfIntegers.get(0), ..., listOfIntegers.get(n));
                                // using your favourite API: JDBC, JPA, Hibernate
  ...
  return resultList;
  
 });
  ``` 
  
  #### Using a Method Reference
  
  ```java
DelayedBatchExecutor2<Integer,String> dbe = DelayedBatchExecutor2.create(Duration.ofMillis(200), 20, this::myBatchCallBack);
  
...
  
List<String> myBatchCallBack(List<Integer> listOfIntegers) {
	List<String>  resultList = ...// execute query:SELECT * FROM TABKE WHERE ID IN (listOfIntegers.get(0), ..., listOfIntegers.get(n));
                                // using your favourite API: JDBC, JPA, Hibernate.
  	...
  	return resultList;
}
``` 
## Execution Policies

There are three policies to use a DelayedBatchExecutor from the code being executed from the threads

### Blocking

The thread is blocked until the result is available, it is implemented by using the method `execute(...)`
 
```java 
    int param = ...
	...
    String result = dbe.execute(param); // this thread will be blocked until the result is available
    // compute with result
```
The following diagram depicts how blocking policy works:

![Blocking image](/src/main/javadoc/doc-files/blocking.svg)


### Non-blocking (Future)

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


### Non-blocking (Reactive using Reactor framework):
 
 The thread is not blocked, it is implemented by using the method `executeAsMono(...)`
 
```java 
    int param1 =...
       ...
    Mono<String> resultMono = dbe.executeAsMono(param1); // the thread will not  be blocked
    // compute something else
    resultMono.subscribe(stringResult -> {
         // compute with stringResult
      }
```
The following diagram depicts how Reactive policy works:

![Reactive image](/src/main/javadoc/doc-files/future.svg)

## Advanced Usage

There are two parameters of a DelayedBatchExecutor that must be known to get the most of it:

- ExecutorService: the callback is actually executed in a parallel thread, which is provided by an java.util.concurrent.ExecutorService. By default this Executor is `Executors.newFixedThreadPool(4)`.

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
Foot note 1:  The example shows a DelayedBatchExecutor for a parameter of type Integer and a return type of String, hence DelayedBatchExecutor2<String,Integer>

For a DelayedBatchExecutor for two parameters (Integer and Date) and a returning type String, the definition would be:
DelayedBatchExecutor3<String,Integer,Date> and so on...
