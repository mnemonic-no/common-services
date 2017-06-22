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

/**
 * A {@link ResultSet} implementation which reads a stream of result batches
 */
public class StreamingResultSetContext implements ResultSet {

  private static final Logger LOGGER = Logging.getLogger(StreamingResultSetContext.class);

  private final RequestHandler handler;
  private final BlockingQueue<Object> values = new LinkedBlockingDeque<>();
  private final int count;
  private final int limit;
  private final int offset;
  private final long maxWait;
  private final AtomicInteger lastIndex = new AtomicInteger();
  private final AtomicBoolean lastMessageReceived = new AtomicBoolean();

  /**
   *
   * @param handler the requesthandler which is controlling the request
   * @param maxWait maximum number of milliseconds to wait for replies (will allow keepalives to extend this)
   * @throws Throwable any exception thrown by the invoked method can be thrown here.
   */
  StreamingResultSetContext(RequestHandler handler, long maxWait) throws Throwable {
    if (handler == null) throw new IllegalArgumentException("Handler is null");
    if (maxWait < 0) throw new IllegalArgumentException("Maxwait must be a non-negative integer");
    this.handler = handler;
    this.maxWait = maxWait;
    ServiceStreamingResultSetResponseMessage b;

    //wait for initial response
    try {
      b = handler.getNextResponse(maxWait);
      if (b == null) throw new ServiceTimeOutException("Initial resultset not received");
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
    if (LOGGER.isDebug()) LOGGER.debug("Got initial batch");
    if (b.getIndex() != 0) throw new ResultSetStreamInterruptedException("Initial resultset message has non-initial index: " + b.getIndex());

    //these values should never change in subsequent resultset batches
    this.count = b.getCount();
    this.limit = b.getLimit();
    this.offset = b.getOffset();

    //accept results from batch
    acceptNextBatch(b);
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
          ServiceStreamingResultSetResponseMessage nextBatch = handler.getNextResponse(maxWait);
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

          if (LOGGER.isDebug()) LOGGER.debug("Got batch idx=%d", nextBatch.getIndex());
          //check that we receive the batches in order
          if (nextBatch.getIndex() != (lastIndex.get() + 1)) {
            handler.close();
            throw new ResultSetStreamInterruptedException(String.format("Resultset batch message has wrong index: %d (expected %d)", nextBatch.getIndex(), lastIndex.get()));
          }

          //accept batch result
          acceptNextBatch(nextBatch);
          return true;

        } catch (UndeclaredThrowableException e) {
          LOGGER.warning(e, "Resultset stream interrupted");
          handler.close();
          throw new ResultSetStreamInterruptedException(e.getUndeclaredThrowable());
        } catch (InvocationTargetException e) {
          LOGGER.warning(e, "Resultset stream interrupted");
          handler.close();
          throw new ResultSetStreamInterruptedException(e.getTargetException());
        } catch (Exception e) {
          LOGGER.warning(e, "Resultset stream interrupted");
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
    values.addAll(nextBatch.getBatch());
    lastIndex.set(nextBatch.getIndex());
    if (nextBatch.isLastMessage()) {
      lastMessageReceived.set(true);
    }
  }

}
