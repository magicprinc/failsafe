/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package dev.failsafe.internal.util;

import dev.failsafe.spi.Scheduler;
import dev.failsafe.testing.Asserts;
import net.jodah.concurrentunit.Waiter;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

@Test
public class DelegatingSchedulerTest {
  Scheduler scheduler = DelegatingScheduler.INSTANCE;

  public void shouldSchedule() throws Throwable {
    // Given
    Duration delay = Duration.ofMillis(200);
    Waiter waiter = new Waiter();
    long startTime = System.nanoTime();

    // When
    scheduler.schedule(() -> {
      waiter.resume();
      return null;
    }, delay.toMillis(), MILLISECONDS);

    // Then
    waiter.await(1000);
    assertTrue(System.nanoTime() - startTime > delay.toNanos());
  }

  public void shouldWrapCheckedExceptions() {
    Asserts.assertThrows(() -> scheduler.schedule(() -> {
      throw new IOException();
    }, 1, MILLISECONDS).get(), ExecutionException.class, IOException.class);
  }

  public void shouldNotInterruptAlreadyDoneTask() throws Throwable {
    Future<?> future1 = scheduler.schedule(() -> null, 0, MILLISECONDS);
    Thread.sleep(100);
    assertFalse(future1.cancel(true));
  }

  /**
   * Asserts that ForkJoinPool clears interrupt flags.
   */
  public void shouldClearInterruptFlagInForkJoinPoolThreads() throws Throwable {
    Scheduler scheduler = new DelegatingScheduler(new ForkJoinPool(1));
    AtomicReference<Thread> threadRef = new AtomicReference<>();
    Waiter waiter = new Waiter();

    // Create interruptable execution
    scheduler.schedule(() -> {
      threadRef.set(Thread.currentThread());
      waiter.resume();
      Thread.sleep(10000);
      return null;
    }, 0, MILLISECONDS);
    waiter.await(1000);
    threadRef.get().interrupt();

    // Check for interrupt flag
    scheduler.schedule(() -> {
      waiter.assertFalse(Thread.currentThread().isInterrupted());
      waiter.resume();
      return null;
    }, 0, MILLISECONDS);
    waiter.await(1000);
  }


  @Test
  public void testInternalPool() throws TimeoutException, ExecutionException, InterruptedException{
    DelegatingScheduler ds = new DelegatingScheduler((byte) 8);// internal, not ForkJoin

    Waiter waiter = new Waiter();

    ScheduledFuture<?> sf = ds.schedule(()->{
      waiter.rethrow(new IOException("OK! testInternalPool"));
      return 42;
    }, 5, MILLISECONDS);

    try {
      waiter.await(1000);
      fail();
    } catch (Throwable e) {
      assertEquals(e.toString(), "java.io.IOException: OK! testInternalPool");
    }
    Thread.sleep(100);// Waiter is too fast :-)
    assertTrue(sf.isDone());

    try {
      sf.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(e.toString(), "java.util.concurrent.ExecutionException: java.io.IOException: OK! testInternalPool");
    }
  }
  /** Internal Pool without flag = as if = external ExecutorService */
  public void testInternalPool2() throws TimeoutException, ExecutionException, InterruptedException{
    DelegatingScheduler ds = new DelegatingScheduler((byte) 0);// "external"
    Waiter waiter = new Waiter();

    ScheduledFuture<?> sf = ds.schedule(()->{
      waiter.rethrow(new IOException("OK! testInternalPool"));
      return 42;
    }, 5, MILLISECONDS);

    try {
      waiter.await(1000);
      fail();
    } catch (Throwable e) {
      assertEquals(e.toString(), "java.io.IOException: OK! testInternalPool");
    }
    assertTrue(sf.isDone());

    try {
      sf.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(e.toString(), "java.util.concurrent.ExecutionException: java.io.IOException: OK! testInternalPool");
    }
  }


  @Test
  public void testExternalScheduler() throws TimeoutException, ExecutionException, InterruptedException{
    ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1);
    DelegatingScheduler ds = new DelegatingScheduler(stpe, stpe);

    Waiter waiter = new Waiter();

    ScheduledFuture<?> sf1 = ds.schedule(()->{
      waiter.rethrow(new IOException("OK! fail 1"));
      return 42;
    }, 3, TimeUnit.SECONDS);
    ScheduledFuture<?> sf2 = ds.schedule(()->{
      waiter.rethrow(new IOException("OK! fail 2 fast"));
      return 42;
    }, 1, TimeUnit.SECONDS);
    assertEquals(1, sf1.compareTo(sf2));
    assertEquals(0, sf1.compareTo(sf1));
    assertTrue(sf1.getDelay(MILLISECONDS) > 2000);

    try {
      waiter.await(1500);// sf2 ~ 1s+
      fail();
    } catch (Throwable e) {
      assertEquals(e.toString(), "java.io.IOException: OK! fail 2 fast", e.toString());
    }
    assertTrue(sf2.isDone());
    assertFalse(sf2.isCancelled());
    Thread.sleep(2500);//3-1 = 2 for slow sf1
    assertTrue(sf1.isDone());
    assertFalse(sf1.isCancelled());

    try {
      sf1.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(e.toString(), "java.util.concurrent.ExecutionException: java.io.IOException: OK! fail 1");
    }
    try {
      sf2.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(e.toString(), "java.util.concurrent.ExecutionException: java.io.IOException: OK! fail 2 fast");
    }
    assertEquals(stpe.shutdownNow().size(), 0);

    assertEquals(-1, sf2.compareTo(sf1));
  }


  @Test
  public void testScheduleAndWork() throws TimeoutException, ExecutionException, InterruptedException{
    Waiter w = new Waiter();
    ScheduledFuture<?> sf1 = DelegatingScheduler.INSTANCE.schedule(()->{
      w.resume();// after ~ 1 sec
      Thread.sleep(5000);//hard work
      w.resume();
      return 42;
    }, 1, TimeUnit.SECONDS);

    ScheduledFuture<?> sf2 = DelegatingScheduler.INSTANCE.schedule(()->112, 3, TimeUnit.SECONDS);

    assertTrue(sf1.getDelay(MILLISECONDS) > 600);
    assertTrue(sf2.getDelay(MILLISECONDS) > 2600);
    assertEquals(-1, sf1.compareTo(sf2));// 1 sec < 3 sec
    assertFalse(sf1.isDone());
    assertFalse(sf2.isDone());

    w.await(1200, 1);// sf1 in normal executor

    assertEquals(-1, sf1.compareTo(sf2));// 1 sec < 3 sec
    assertEquals(sf1.getDelay(MILLISECONDS), 0);
    assertTrue(sf2.getDelay(MILLISECONDS) > 1600);
    assertFalse(sf1.isDone());
    assertFalse(sf2.isDone());

    w.await(5200, 1);// sf2 is done

    assertEquals(0, sf1.compareTo(sf2));// no more time info inside
    assertEquals(sf1.getDelay(MILLISECONDS), 0);
    assertEquals(sf2.getDelay(MILLISECONDS), 0);
    assertTrue(sf1.isDone());
    assertTrue(sf2.isDone());
    assertEquals(42, sf1.get());
    assertEquals(112, sf2.get());
  }

  @Test
  public void testScheduleAndCancel() throws TimeoutException, ExecutionException, InterruptedException{
    Waiter w = new Waiter();
    ScheduledFuture<?> sf1 = DelegatingScheduler.INSTANCE.schedule(()->{
      w.resume();// after ~ 1 sec
      Thread.sleep(5000);//hard work
      w.resume();
      return 42;
    }, 1, TimeUnit.SECONDS);

    ScheduledFuture<?> sf2 = DelegatingScheduler.INSTANCE.schedule(()->112, 3, TimeUnit.SECONDS);

    assertTrue(sf1.getDelay(MILLISECONDS) > 600);
    assertTrue(sf2.getDelay(MILLISECONDS) > 2600);
    assertEquals(-1, sf1.compareTo(sf2));// 1 sec < 3 sec
    assertFalse(sf1.isDone());
    assertFalse(sf2.isDone());

    w.await(1200, 1);// sf1 in normal executor

    assertEquals(-1, sf1.compareTo(sf2));// 1 sec < 3 sec
    assertEquals(sf1.getDelay(MILLISECONDS), 0);
    assertTrue(sf2.getDelay(MILLISECONDS) > 1600);
    assertFalse(sf1.isDone());
    assertFalse(sf2.isDone());

    sf1.cancel(true);
    sf2.cancel(true);

    assertEquals(-1, sf1.compareTo(sf2));// time info inside in sf2's delegate
    assertEquals(sf1.getDelay(MILLISECONDS), 0);
    assertTrue(sf2.getDelay(MILLISECONDS) > 1000);
    assertTrue(sf1.isDone());
    assertTrue(sf2.isDone());
    assertTrue(sf1.isCancelled());
    assertTrue(sf2.isCancelled());
    CancellationException c1 = expectThrows(CancellationException.class, sf1::get);
    CancellationException c2 = expectThrows(CancellationException.class, sf2::get);
    DelegatingScheduler.ScheduledCompletableFuture<?> scf1 = (DelegatingScheduler.ScheduledCompletableFuture<?>) sf1;
    DelegatingScheduler.ScheduledCompletableFuture<?> scf2 = (DelegatingScheduler.ScheduledCompletableFuture<?>) sf2;

    assertTrue(scf1.delegate instanceof ForkJoinTask);// was executing
    ForkJoinTask<?> task1 = (ForkJoinTask<?>) scf1.delegate;
    assertTrue(scf2.delegate instanceof RunnableScheduledFuture);// was in scheduler's delayQueue
    RunnableScheduledFuture<?> task2 = (RunnableScheduledFuture<?>) scf2.delegate;
    assertTrue(task1.isCompletedAbnormally());
    assertTrue(task1.isCancelled());
    assertTrue(task2.isCancelled());
  }

  public void testWaitTwice () throws TimeoutException, ExecutionException, InterruptedException{
    DelegatingScheduler ds = new DelegatingScheduler((byte) 0);

    long t = System.nanoTime();
    DelegatingScheduler.ScheduledCompletableFuture<?> sf = (DelegatingScheduler.ScheduledCompletableFuture<?>)
        ds.schedule(()->
    {
      Thread.sleep(1000);
      return 42;
    }, 500, MILLISECONDS);// 500 + 1000 = min 1500ms

    assertEquals(sf.get(), 42);
    assertTrue(sf.isDone());
    assertFalse(sf.isCancelled());
    t = (System.nanoTime() - t)/1_000_000;//to millis
    assertTrue(t >= 1500);

    assertTrue(sf.delegate instanceof ForkJoinTask);
    assertTrue(sf.delegate.isDone());
    assertNull(sf.delegate.get());
  }

  public void testWaitTwiceWithTimeout1ok () throws TimeoutException, ExecutionException, InterruptedException{
    DelegatingScheduler ds = new DelegatingScheduler((byte) 0);

    long t = System.nanoTime();
    ScheduledFuture<?> sf = ds.schedule(()->{
      Thread.sleep(1000);
      return 42;
    }, 500, MILLISECONDS);

    assertEquals(sf.get(1700, MILLISECONDS), 42);
    assertTrue(sf.isDone());
    assertFalse(sf.isCancelled());
    t = (System.nanoTime() - t)/1_000_000;//to millis
    assertTrue(t >= 1500);
  }

  public void testWaitTwiceWithTimeout2 () throws ExecutionException, InterruptedException{
    DelegatingScheduler ds = new DelegatingScheduler((byte) 0);

    long t = System.nanoTime();
    ScheduledFuture<?> sf = ds.schedule(()->{
      Thread.sleep(5000);
      return 42;
    }, 500, MILLISECONDS);

    try {
      sf.get(100, MILLISECONDS);// Timeout in Scheduled
      fail();
    } catch (TimeoutException e) {
      assertEquals(e.toString(), "java.util.concurrent.TimeoutException");
    }
    assertTrue(((DelegatingScheduler.ScheduledCompletableFuture<?>)sf).delegate instanceof RunnableScheduledFuture);
    assertFalse(sf.isDone());
    assertFalse(sf.isCancelled());
    t = (System.nanoTime() - t)/1_000_000;//to millis
    assertTrue(t > 100 && t < 500);
    assertEquals(sf.get(), 42);
    assertTrue(sf.isDone());
    assertFalse(sf.isCancelled());
  }

  public void testWaitTwiceWithTimeout3 () throws TimeoutException, ExecutionException, InterruptedException{
    DelegatingScheduler ds = new DelegatingScheduler((byte) 0);

    long t = System.nanoTime();
    ScheduledFuture<?> sf = ds.schedule(()->{
      Thread.sleep(1500);
      return 42;
    }, 300, MILLISECONDS);

    try {
      sf.get(900, MILLISECONDS);// Timeout in target executor
    } catch (TimeoutException e) {
      assertEquals(e.toString(), "java.util.concurrent.TimeoutException");
    }
    assertTrue(((DelegatingScheduler.ScheduledCompletableFuture<?>)sf).delegate instanceof ForkJoinTask);
    assertFalse(sf.isDone());
    assertFalse(sf.isCancelled());
    t = (System.nanoTime() - t)/1_000_000;//to millis
    assertTrue(t >= 900 && t < 1500);
    assertEquals(sf.get(), 42);
    assertTrue(sf.isDone());
    assertFalse(sf.isCancelled());
  }

  public void testDontWrapIfSpectial () throws TimeoutException, InterruptedException{
    DelegatingScheduler ds = new DelegatingScheduler((byte) 0);

    ScheduledFuture<?> sf = ds.schedule(()-> {
      Thread.sleep(500);
      throw new ExecutionException("It's me", new IOException("fake"));
    }, 100, MILLISECONDS);

    try {
      sf.get();
      fail();
    } catch (ExecutionException e) {
      assertEquals(e.toString(), "java.util.concurrent.ExecutionException: It's me", e.toString());
      assertEquals(e.getCause().toString(), "java.io.IOException: fake", e.getCause().toString());
    }
    assertTrue(sf.isDone());
    assertFalse(sf.isCancelled());
  }


  @Test public void testUsedMemory() throws ExecutionException, InterruptedException, TimeoutException{
    { Runtime rt = Runtime.getRuntime();
    rt.gc(); rt.gc(); rt.gc();
    Thread.sleep(5000); }
    final long usedMemory0 = getUsedMemory();

    final int MAX = 5_000_000;//10m = same result
    final int TIMEOUT = 5_000;
    //to control queue:
    ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(1, r->{
      Thread t = new Thread(r, "PUSHER");
      t.setDaemon(true);
      t.setPriority(Thread.MAX_PRIORITY);
      return t;
    });
    DelegatingScheduler ds = new DelegatingScheduler(null/*main common pool*/, es);
    // task is the same = constant memory
    final Callable<Object> businessTask = ()->42;

    final long deadline = System.currentTimeMillis() + TIMEOUT;

    final ScheduledFuture<?> sf0 = ds.schedule(businessTask, TIMEOUT, MILLISECONDS);
    ScheduledFuture<?> sf = null;
    for (int i=0; i<MAX; i++){
      sf = ds.schedule(businessTask, deadline-System.currentTimeMillis(), MILLISECONDS);
    }
    assertFalse(sf0.isDone());
    assertFalse(sf0.isCancelled());
    assertFalse(sf.isDone());
    assertFalse(sf.isCancelled());

    while (!es.getQueue().isEmpty()){
      long usedMemory1 = getUsedMemory() - usedMemory0;
      System.out.println(deadline-System.currentTimeMillis());
      System.out.println("USED MEMORY = "+(usedMemory1/1024/1024.0)+" ~ "+(usedMemory1/MAX));
      System.out.println("Q size: " + es.getQueue().size() + ", ActiveCount: "+es.getActiveCount()+
          ", TaskCount: "+es.getTaskCount()+", PoolSize: "+es.getPoolSize());
      Thread.sleep(2000);// don't be noisy
    }

    assertEquals(42, sf0.get());
    assertTrue(sf0.isDone());
    assertFalse(sf0.isCancelled());
    assertEquals(42, sf.get());
    assertTrue(sf.isDone());
    assertFalse(sf.isCancelled());

    long usedMemory2 = getUsedMemory() - usedMemory0;

    System.out.println("USED MEMORY 2 = "+(usedMemory2/1024/1024.0)+" ~ "+(usedMemory2/MAX));
  }

  public static long getUsedMemory() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }


  @Test public void testTransferSpeed() throws InterruptedException, ExecutionException{
    final int MAX = 5_000_000;//10m = same result
    final int TIMEOUT = 5_000;
    //to control queue:
    ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(1, r->{
      Thread t = new Thread(r, "PUSHER");
      t.setDaemon(true);
      t.setPriority(Thread.MAX_PRIORITY);
      return t;
    });
    final Future<?> DONE_FUTURE = new Future<Object>(){
      @Override public boolean cancel (boolean mayInterruptIfRunning){ return true;}
      @Override public boolean isCancelled (){ return false;}
      @Override public boolean isDone (){ return true;}
      @Override public Object get (){ return null;}
      @Override public Object get (long timeout, TimeUnit unit){ return null;}
    };
    AtomicLong processed = new AtomicLong();
    final ExecutorService main = new ExecutorService(){
      @Override public void shutdown (){}
      @Override public List<Runnable> shutdownNow (){ return null;}
      @Override public boolean isShutdown (){ return false;}
      @Override public boolean isTerminated (){ return false;}
      @Override public boolean awaitTermination (long timeout, TimeUnit unit){return false;}
      @SuppressWarnings("unchecked") @Override public <T> Future<T> submit (Callable<T> task){
        processed.incrementAndGet();
        try {
          task.call();
        } catch (Throwable e) {
          throw new AssertionError(e);
        }
        return (Future<T>) DONE_FUTURE;
      }
      @Override public <T> Future<T> submit (Runnable task, T result){
        throw new AssertionError("submit: "+task);
      }
      @Override public Future<?> submit (Runnable task){throw new AssertionError("submit: "+task);}
      @Override public <T> List<Future<T>> invokeAll (Collection<? extends Callable<T>> tasks){
        throw new AssertionError("invokeAll: "+tasks);
      }
      @Override public <T> List<Future<T>> invokeAll (Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit){
        throw new AssertionError("invokeAll: "+tasks);
      }
      @Override public <T> T invokeAny (Collection<? extends Callable<T>> tasks){
        throw new AssertionError("invokeAny: "+tasks);
      }
      @Override public <T> T invokeAny (Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit){
        throw new AssertionError("invokeAny: "+tasks);
      }
      @Override public void execute (Runnable command){throw new AssertionError("execute: "+command);}
    };

    DelegatingScheduler ds = new DelegatingScheduler(main, es);
    // task is the same = constant memory
    final Callable<Object> businessTask = ()->42;

    final long deadline = System.currentTimeMillis() + TIMEOUT;

    final ScheduledFuture<?> sf0 = ds.schedule(businessTask, TIMEOUT, MILLISECONDS);
    ScheduledFuture<?> sf = null;
    for (int i=0; i<MAX-1; i++){
      sf = ds.schedule(businessTask, deadline-System.currentTimeMillis(), MILLISECONDS);
    }

    int i = 0;
    while (processed.get() < MAX){
      if (i++ % 100 == 0) {
        System.out.println(deadline - System.currentTimeMillis());
        System.out.println("Q size: " + es.getQueue().size() + ", ActiveCount: " + es.getActiveCount() +
            ", TaskCount: " + es.getTaskCount() + ", PoolSize: " + es.getPoolSize() + ", processed: " + processed);
      }
      Thread.sleep(20);// don't be noisy
    }
    long t = System.currentTimeMillis() - deadline;

    assertEquals(processed.get(), MAX);//sf0+the rest
    assertEquals(sf0.get(), 42);
    assertTrue(sf0.isDone());
    assertFalse(sf0.isCancelled());
    assertSame(((DelegatingScheduler.ScheduledCompletableFuture<?>)sf0).delegate, DONE_FUTURE);
    assertSame(((DelegatingScheduler.ScheduledCompletableFuture<?>)sf).delegate, DONE_FUTURE);

    System.out.println("Total transfer time (ms): "+t);// Total transfer time (ms): 9851
    System.out.println("Task/sec ~ "+(MAX*1000.0/t));// Task/sec ~ 507_563
  }
}