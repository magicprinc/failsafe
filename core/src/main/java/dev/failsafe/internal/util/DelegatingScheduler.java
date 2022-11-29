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

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.ForkJoinPool.commonPool;

/**
 * A {@link Scheduler} implementation that schedules delays on an internal, common ScheduledExecutorService and executes
 * tasks on either a provided ExecutorService, {@link ForkJoinPool#commonPool()}, or an internal {@link ForkJoinPool}
 * instance. If no {@link ExecutorService} is supplied, the {@link ForkJoinPool#commonPool()} will be used, unless the
 * common pool's parallelism is 1, then an internal {@link ForkJoinPool} with parallelism of 2 will be created and
 * used.
 * <p>
 * Supports cancellation of {@link ForkJoinPool} tasks.
 * </p>
 *
 * @author Jonathan Halterman
 * @author Ben Manes
 */
public final class DelegatingScheduler implements Scheduler {
  public static final DelegatingScheduler INSTANCE = new DelegatingScheduler();

  private final ExecutorService executorService;
  private final int executorType;

  private static final int EX_FORK_JOIN = 1;
  private static final int EX_SCHEDULED = 2;
  private static final int EX_COMMON    = 4;
  private static final int EX_INTERNAL  = 8;


  private DelegatingScheduler() {
    this(null, false);
  }

  public DelegatingScheduler(ExecutorService executor) {
    this(executor, false);
  }

  public DelegatingScheduler(ExecutorService executor, boolean canUseScheduledExecutorService) {
    final int type;
    if (executor == null || executor == commonPool()) {
      if (ForkJoinPool.getCommonPoolParallelism() > 1) {// @see CompletableFuture#useCommonPool
        executorService = commonPool();
        type = EX_COMMON   | EX_FORK_JOIN;

      } else {// don't use commonPool(): cannot support parallelism
        executorService = null;
        type = EX_INTERNAL | EX_FORK_JOIN;
      }
    } else {
      executorService = executor;
      type = executor instanceof ForkJoinPool ? EX_FORK_JOIN
          : 0;
    }
    executorType = canUseScheduledExecutorService && executorService instanceof ScheduledExecutorService
        ? type | EX_SCHEDULED
        : type;
  }

  DelegatingScheduler (byte flags) {
    executorService = null;
    executorType = flags;
  }//new for tests

  private static final class LazyDelayerHolder extends ScheduledThreadPoolExecutor implements ThreadFactory {
    private static final ScheduledThreadPoolExecutor DELAYER = new LazyDelayerHolder();

    public LazyDelayerHolder(){
      super(1);
      setThreadFactory(this);
      setRemoveOnCancelPolicy(true);
    }

    @Override public Thread newThread(Runnable r) {
      Thread t = new Thread(r, "FailsafeDelayScheduler");
      t.setDaemon(true);
      return t;
    }
  }

  private static final class LazyForkJoinPoolHolder {
    private static final ForkJoinPool FORK_JOIN_POOL = new ForkJoinPool(
        Math.max(Runtime.getRuntime().availableProcessors(), 2),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null, true/*asyncMode*/);
  }

  static class ScheduledCompletableFuture<V> implements ScheduledFuture<V>, Callable<V>{
    // Guarded by this
    volatile Future<V> delegate;
    // Guarded by this
    Thread forkJoinPoolThread;
    volatile Object res;
    final Callable<?> callable;

    public ScheduledCompletableFuture(Callable<?> callable){
      this.callable = callable;
      res = this; // WORKING
    }

    @Override public long getDelay(TimeUnit unit){
      Future<V> f = delegate;
      return f instanceof Delayed ? ((Delayed) f).getDelay(unit)
          : 0; // we are executed now
    }

    @Override
    public int compareTo(Delayed other) {
      if (other == this)// ScheduledFuture<?> gives no extra info
        return 0;
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      boolean result = res instanceof CancellationException;

      if (delegate != null)
        result |= delegate.cancel(mayInterruptIfRunning);

      if (forkJoinPoolThread != null && mayInterruptIfRunning)
        forkJoinPoolThread.interrupt();

      if (result) {
        if (!(res instanceof CancellationException) && (res == this)) {
          res = new CancellationException();
        }
      }
      return result;
    }

    @Override public boolean isCancelled(){
      final Future<V> f = delegate;
      return (res instanceof CancellationException)
          || (f != null && f.isCancelled());
    }

    @Override public boolean isDone(){
      final Future<V> f = delegate;
      return (res instanceof CancellationException)
          || res != this
          || (f != null && f.isCancelled())
          || (f != null && f.isDone());
    }

    @Override @SuppressWarnings("unchecked")
    public V get() throws InterruptedException, ExecutionException{
      if (res == this) {// WORKING
        Future<?> f = delegate;
        if (f != null)
          f.get();// returns null, but we need not result, but blocking wait
        // 2nd executor
        if (res == this) {// WORKING
          f = delegate;
          if (f != null)
            f.get();
        }
      }
      Object r = res;
      if (r instanceof CancellationException)
        throw (CancellationException) r;
      if (r instanceof ExecutionException)
        throw (ExecutionException) r;
      return (V) r;
    }

    @Override @SuppressWarnings("unchecked")
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException{
      if (res == this) {// WORKING
        Future<?> f = delegate;
        if (f != null)
          f.get(timeout,unit);// returns null, but we need not result, but blocking wait
        // 2nd executor
        if (res == this) {// WORKING
          f = delegate;
          if (f != null)
            f.get(timeout,unit);
        }
      }
      Object r = res;
      if (r instanceof CancellationException)
        throw (CancellationException) r;
      if (r instanceof ExecutionException)
        throw (ExecutionException) r;
      return (V) r;
    }

    protected void before () {}
    protected void after () {}

    @Override public V call() {
      try {
        before();
        res = callable.call(); // good result is above all principles :-)
      } catch (Throwable e) {
        if (res == this) {//WORKING: not completed, not canceled
          if (e instanceof CancellationException || e instanceof ExecutionException) {
            res = e;// as is
          } else if (e instanceof InterruptedException) {
            CancellationException tmp = new CancellationException();
            tmp.initCause(e);
            res = tmp;
          } else {
            res = new ExecutionException(e);
          }
        }
      } finally {
        after();
      }
      return null;
    }
  }//ScheduledCompletableFuture

  static class ScheduledCompletableFutureFJ<V> extends ScheduledCompletableFuture<V> {
    public ScheduledCompletableFutureFJ(Callable<V> callable){
      super(callable);
    }

    @Override protected synchronized void before(){
      // Guard against race with promise.cancel
      forkJoinPoolThread = Thread.currentThread();
    }

    @Override protected synchronized void after(){
      forkJoinPoolThread = null;
    }
  }

  private ScheduledExecutorService delayer() {
    return ((executorType & EX_SCHEDULED) == EX_SCHEDULED) ? (ScheduledExecutorService) executorService()
        : LazyDelayerHolder.DELAYER;
  }

  private ExecutorService executorService() {
    return executorService != null ? executorService
        : LazyForkJoinPoolHolder.FORK_JOIN_POOL;
  }

  @Override @SuppressWarnings({"rawtypes", "unchecked"})
  public ScheduledFuture<?> schedule(Callable<?> callable, long delay, TimeUnit unit) {
    ScheduledCompletableFuture promise = (executorType & EX_FORK_JOIN) == EX_FORK_JOIN
        ? new ScheduledCompletableFutureFJ(callable)
        : new ScheduledCompletableFuture(callable);

    if (delay <= 0) {
      promise.delegate = executorService().submit(promise);
      return promise;
    }
    final ExecutorService es = executorService();
    final Runnable r;// use less memory: don't capture variable with commonPool

    if ((executorType & EX_COMMON) == EX_COMMON)
      r = ()->{
        // Guard against race with promise.cancel
        synchronized(promise) {
          if (!promise.isCancelled())
            promise.delegate = commonPool().submit(promise);
        }
      };

    else if ((executorType & EX_INTERNAL) == EX_INTERNAL)
      r = ()->{
        // Guard against race with promise.cancel
        synchronized(promise) {
          if (!promise.isCancelled())
            promise.delegate = LazyForkJoinPoolHolder.FORK_JOIN_POOL.submit(promise);
        }
      };

    else
      r = ()->{
        // Guard against race with promise.cancel
        synchronized(promise) {
          if (!promise.isCancelled())
            promise.delegate = es.submit(promise);
        }
      };
    promise.delegate = delayer().schedule(r, delay, unit);
    return promise;
  }
}