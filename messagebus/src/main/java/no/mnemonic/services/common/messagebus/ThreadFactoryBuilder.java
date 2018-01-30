package no.mnemonic.services.common.messagebus;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNull;

/**
 * TODO: Move this class to commons, to avoid having to pull inn Guava or Apache commons for this.
 */
class ThreadFactoryBuilder {

  private static final AtomicInteger poolNumber = new AtomicInteger(1);

  private String namePrefix;

  ThreadFactory build() {
    return new ThreadFactoryWithNamePrefix(namePrefix);
  }

  ThreadFactoryBuilder setNamePrefix(String namePrefix) {
    this.namePrefix = namePrefix;
    return this;
  }

  private static class ThreadFactoryWithNamePrefix implements ThreadFactory {

    private final ThreadGroup group;
    private final String namePrefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    /**
     * Creates a new ThreadFactory where threads are created with a name prefix
     * of <code>prefix</code>.
     *
     * @param prefix Thread name prefix. Never use a value of "pool" as in that
     *               case you might as well have used
     *               {@link java.util.concurrent.Executors#defaultThreadFactory()}.
     */
    private ThreadFactoryWithNamePrefix(String prefix) {
      group = ifNotNull(System.getSecurityManager(), SecurityManager::getThreadGroup, Thread.currentThread().getThreadGroup());
      namePrefix = prefix + "-" + poolNumber.getAndIncrement() + "-thread-";
    }


    @Override
    public Thread newThread(Runnable runnable) {
      Thread t = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement(), 0);
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }
}
