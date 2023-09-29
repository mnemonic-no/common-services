package no.mnemonic.services.common.messagebus;

public interface ServiceContext {

  enum Priority {
    bulk,
    standard,
    expedite;
  }

  /**
   * Set the request priority to use for the current thread
   * @param priority the priority to use for this thread
   */
  void setThreadPriority(Priority priority);

  /**
   * Set the request priority to use for the current thread, for the next request.
   * The requested priority will be cleared after first use, and this thread will fall back to
   * the thread priority, or the default priority.
   *
   * @param priority the priority to use for this thread for the next request
   */
  void setNextRequestPriority(Priority priority);

  /**
   * Set the response window size to use for the current thread, for the next request.
   * This limits the number of streaming responses in flight, and thereby limits the per-request memory consumption,
   * for clients which use RequestSink protocol version V4.
   *
   * The requested window size will be cleared after first use, and this thread will fall back to the
   * default response size.
   *
   * Service clients may check this interface defensively (in case the client is indeed not a ServiceProxy)
   * <code>
   *   if (service instanceof ServiceProxy) {
   *     ((ServiceProxy)service).getServiceContext().setNextResponseWindowSize(10);
   *   }
   * </code>
   *
   * @param responseWindowSize The response segment window size to negotiate with the server
   */
  void setNextResponseWindowSize(int responseWindowSize);
}
