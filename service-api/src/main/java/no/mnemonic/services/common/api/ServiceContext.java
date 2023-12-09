package no.mnemonic.services.common.api;

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

}
