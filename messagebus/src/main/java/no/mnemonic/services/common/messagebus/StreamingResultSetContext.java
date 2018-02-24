package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.requestsink.RequestHandler;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetStreamInterruptedException;
import no.mnemonic.services.common.api.ServiceTimeOutException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A {@link ResultSet} implementation which reads a stream of result batches
 */
public class StreamingResultSetContext implements ResultSet {

  private static final Logger LOGGER = Logging.getLogger(StreamingResultSetContext.class);
  private static final String RESULTSET_STREAM_INTERRUPTED = "Resultset stream interrupted";

  private final Consumer<Throwable> onError;
  private final RequestHandler handler;
  private final BlockingQueue<Object> values = new LinkedBlockingDeque<>();
  private final int count;
  private final int limit;
  private final int offset;
  private final AtomicInteger lastIndex = new AtomicInteger();
  private final AtomicBoolean lastMessageReceived = new AtomicBoolean();

  /**
   *
   * @param handler the requesthandler which is controlling the request
   * @throws Throwable any exception thrown by the invoked method can be thrown here.
   */
  StreamingResultSetContext(RequestHandler handler, Consumer<Throwable> onError) throws Throwable {
    if (handler == null) throw new IllegalArgumentException("Handler is null");
    if (onError == null) throw new IllegalArgumentException("Error consumer is null");
    this.handler = handler;
    this.onError = onError;
    ServiceStreamingResultSetResponseMessage initialBatch;

    //wait for initial response
    try {
      initialBatch = handler.getNextResponse();
      if (initialBatch == null) {
        //notify timeout
        handler.timeout();
        throw new ServiceTimeOutException("Initial resultset not received");
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

    //accept results from batch
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
  public Iterator iterator() {
    return new Iterator() {

      @Override
      public boolean hasNext() {
        try {
          //if there are unfetched values in queue, return true
          if (!values.isEmpty()) return true;
          //stream is already normally closed
          if (handler.isClosed() && lastMessageReceived.get()) return false;

          //check handler for more data
          ServiceStreamingResultSetResponseMessage nextBatch = handler.getNextResponse();
          //timeout/EOS occurred
          if (nextBatch == null) {
            if (!handler.isClosed()) {
              throw new ResultSetStreamInterruptedException("Stream unexpectedly closed");
            }
            if (!lastMessageReceived.get()) {
              throw new ResultSetStreamInterruptedException(String.format("Inconsistent batch messages: lastMessage never received (last index=%d)", lastIndex.get()));
            }
            //handler is closed and we have received last message, so close stream normally
            return false;
          } else {
            //if there are more messages from handler, we really should not have received lastMessage already
            if (lastMessageReceived.get()) {
              throw new ResultSetStreamInterruptedException(String.format("Inconsistent batch messages: lastMessage already received (idx=%d)", nextBatch.getIndex()));
            }
          }

          //check that we receive the batches in order
          if (nextBatch.getIndex() != (lastIndex.get() + 1)) {
            handler.close();
            throw new ResultSetStreamInterruptedException(String.format("Resultset batch message has wrong index: %d (expected %d)", nextBatch.getIndex(), lastIndex.get()));
          }

          //accept batch result
          acceptNextBatch(nextBatch);
          return true;

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
      public Object next() {
        if (!hasNext()) throw new NoSuchElementException("At end of stream");
        return values.poll();
      }
    };
  }

  private void acceptNextBatch(ServiceStreamingResultSetResponseMessage nextBatch) {
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< streamingResult [callID=%s idx=%d count=%d]", nextBatch.getCallID(), nextBatch.getIndex(), nextBatch.getCount());
    }
    values.addAll(nextBatch.getBatch());
    lastIndex.set(nextBatch.getIndex());
    if (nextBatch.isLastMessage()) {
      lastMessageReceived.set(true);
    }
  }

}
