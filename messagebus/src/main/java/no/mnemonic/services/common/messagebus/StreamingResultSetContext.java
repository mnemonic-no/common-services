package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.RequestHandler;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetStreamInterruptedException;
import no.mnemonic.services.common.api.ServiceTimeOutException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A {@link ResultSet} implementation which reads a stream of result batches
 */
public class StreamingResultSetContext<T> implements ResultSet<T> {

  private static final Logger LOGGER = Logging.getLogger(StreamingResultSetContext.class);
  private static final String RESULTSET_STREAM_INTERRUPTED = "Resultset stream interrupted";

  private final Consumer<Throwable> onError;
  private final RequestHandler handler;
  private final BlockingQueue<T> values = new LinkedBlockingDeque<>();
  private final int count;
  private final int limit;
  private final int offset;
  private final AtomicInteger lastIndex = new AtomicInteger();
  private final AtomicBoolean lastMessageReceived = new AtomicBoolean();
  private final Map<Integer, ServiceStreamingResultSetResponseMessage> waitingBatches = new HashMap<>();

  /**
   * @param handler       the requesthandler which is controlling the request
   * @param invokedMethod the invoked method
   * @throws Throwable any exception thrown by the invoked method can be thrown here.
   */
  StreamingResultSetContext(RequestHandler handler, Method invokedMethod, Consumer<Throwable> onError) throws Throwable {
    if (handler == null) throw new IllegalArgumentException("Handler is null");
    if (onError == null) throw new IllegalArgumentException("Error consumer is null");
    if (invokedMethod == null) throw new IllegalArgumentException("invokedMethod is null");
    this.handler = handler;
    this.onError = onError;
    ServiceStreamingResultSetResponseMessage initialBatch;

    //wait for initial response
    try {
      initialBatch = handler.getNextResponse();
      if (initialBatch == null) {
        //notify timeout
        handler.timeout();
        LOGGER.error("ServiceTimeoutException while waiting for initial resultset for method %s", invokedMethod);
        throw new ServiceTimeOutException(
            String.format("Initial resultset not received for method %s", invokedMethod),
            invokedMethod.getDeclaringClass().getName()
        );
      }
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
    //do not register error here, since this thread is called from ServiceMessageClient, which will register the error
    if (initialBatch.getIndex() != 0) throw new ResultSetStreamInterruptedException("Initial resultset message has non-initial index: " + initialBatch.getIndex());

    //these values should never change in subsequent resultset batches
    this.count = initialBatch.getCount();
    this.limit = initialBatch.getLimit();
    this.offset = initialBatch.getOffset();

    //accept results from initial batch
    acceptNextBatch(initialBatch);
  }

  @Override
  public int getCount() {
    return count;
  }

  @Override
  public int getLimit() {
    return limit;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public void close() {
    handler.close();
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {

      @Override
      public boolean hasNext() {
        try {
          //if there are unfetched values in queue, return true
          if (!values.isEmpty()) return true;
          //stream is already normally closed
          if (handler.isClosed() && lastMessageReceived.get()) return false;
          return !checkForNextBatch();
        } catch (UndeclaredThrowableException e) {
          LOGGER.warning(e, RESULTSET_STREAM_INTERRUPTED);
          onError.accept(e);
          handler.close();
          throw new ResultSetStreamInterruptedException(e.getUndeclaredThrowable());
        } catch (InvocationTargetException e) {
          LOGGER.warning(e, RESULTSET_STREAM_INTERRUPTED);
          onError.accept(e);
          handler.close();
          throw new ResultSetStreamInterruptedException(e.getTargetException());
        } catch (Exception e) {
          LOGGER.warning(e, RESULTSET_STREAM_INTERRUPTED);
          onError.accept(e);
          handler.close();
          throw new ResultSetStreamInterruptedException(e);
        }
      }

      @Override
      public T next() {
        if (!hasNext()) throw new NoSuchElementException("At end of stream");
        return values.poll();
      }
    };
  }

  private boolean checkForNextBatch() throws InvocationTargetException {
    //check handler for more data
    ServiceStreamingResultSetResponseMessage nextBatch = fetchNextBatch();

    //timeout/EOS occurred
    if (nextBatch == null) {
      if (!waitingBatches.isEmpty()) {
        LOGGER.warning("Still %d batches in waitingBatches when closing stream", waitingBatches.size());
        waitingBatches.clear();
      }
      if (!handler.isClosed()) {
        throw new ResultSetStreamInterruptedException("Stream unexpectedly closed");
      }
      if (!lastMessageReceived.get()) {
        throw new ResultSetStreamInterruptedException(String.format("Inconsistent batch messages: lastMessage never received (last index=%d)", lastIndex.get()));
      }
      //handler is closed and we have received last message, so close stream normally
      return true;
    }

    //accept batch result
    acceptNextBatch(nextBatch);
    return false;
  }

  private ServiceStreamingResultSetResponseMessage fetchNextBatch() throws InvocationTargetException {
    int expectedIndex = lastIndex.get() + 1;
    //if the batch we want next is previously received (out-of-order), return it
    if (waitingBatches.containsKey(expectedIndex)) return waitingBatches.remove(expectedIndex);
    //try fetching next from stream
    ServiceStreamingResultSetResponseMessage nextResponse = handler.getNextResponse();
    if (nextResponse == null) return null;
    //if we receive a batch out-of-order, put it into waitingBatches and try again
    if (nextResponse.getIndex() != expectedIndex) {
      waitingBatches.put(nextResponse.getIndex(), nextResponse);
      return fetchNextBatch();
    }
    return nextResponse;
  }

  private void acceptNextBatch(ServiceStreamingResultSetResponseMessage nextBatch) {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< streamingResult [callID=%s idx=%d count=%d]", nextBatch.getCallID(), nextBatch.getIndex(), nextBatch.getCount());
    }
    //noinspection unchecked
    values.addAll((Collection<? extends T>) nextBatch.getBatch());
    lastIndex.set(nextBatch.getIndex());
    if (nextBatch.isLastMessage()) {
      lastMessageReceived.set(true);
    }
  }

}
