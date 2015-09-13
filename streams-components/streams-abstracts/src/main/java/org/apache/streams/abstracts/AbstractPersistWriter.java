package org.apache.streams.abstracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.core.StreamsPersistWriter;
import org.apache.streams.jackson.StreamsJacksonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by smartin on 6/25/15.
 */
public abstract class AbstractPersistWriter<T> implements StreamsPersistWriter, Runnable {

    public static final String STREAMS_ID = AbstractPersistWriter.class.getCanonicalName();
    protected final static Logger LOGGER = LoggerFactory.getLogger(AbstractPersistWriter.class);
    private final static long MAX_WRITE_LATENCY = 1000;
    public static final int BATCH_SIZE = 100;
    protected final ReadWriteLock lock = new ReentrantReadWriteLock();
    protected volatile Queue<StreamsDatum> persistQueue;
    protected List<T> insertBatch = Lists.newArrayList();
    protected ObjectMapper mapper = StreamsJacksonMapper.getInstance();
    private volatile AtomicLong lastWrite = new AtomicLong(System.currentTimeMillis());
    private ScheduledExecutorService backgroundFlushTask = Executors.newSingleThreadScheduledExecutor();

    public void setPersistQueue(Queue<StreamsDatum> persistQueue) {
        this.persistQueue = persistQueue;
    }

    public Queue<StreamsDatum> getPersistQueue() {
        return persistQueue;
    }

    @Override
    public void write(StreamsDatum streamsDatum) {

        T object = prepareObject(streamsDatum);
        if (object != null) {
            addToBatch(object);
            flushIfNecessary();
        }
    }

    public void flush() throws IOException {
        try {
            LOGGER.debug("Attempting to flush {} items", insertBatch.size());
            lock.writeLock().lock() ;
            writeList(insertBatch);
            lastWrite.set(System.currentTimeMillis());
            insertBatch = Lists.newArrayList();
        } finally {
            lock.writeLock().unlock();
        }

    }


    public synchronized void close() throws IOException {
        cleanUp();
        backgroundFlushTask.shutdownNow();
    }

    @Override
    public void cleanUp() {
        stop();
    }

    public void start() {
        connect();
        backgroundFlushTask.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flushIfNecessary();
            }
        }, 0, MAX_WRITE_LATENCY * 2, TimeUnit.MILLISECONDS);
    }

    public void stop() {

        try {
            flush();
        } catch (IOException e) {
            LOGGER.error("Error flushing", e);
        }
        try {
            close();
        } catch (IOException e) {
            LOGGER.error("Error closing", e);
        }
        try {
            backgroundFlushTask.shutdown();
            // Wait a while for existing tasks to terminate
            if (!backgroundFlushTask.awaitTermination(15, TimeUnit.SECONDS)) {
                backgroundFlushTask.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!backgroundFlushTask.awaitTermination(15, TimeUnit.SECONDS)) {
                    LOGGER.error("Stream did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            backgroundFlushTask.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

    }

    @Override
    public void run() {

        while (true) {
            if (persistQueue.peek() != null) {
                try {
                    StreamsDatum entry = persistQueue.remove();
                    write(entry);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(new Random().nextInt(1));
            } catch (InterruptedException e) {
            }
        }

    }

    @Override
    public void prepare(Object configurationObject) {
        this.persistQueue = new ConcurrentLinkedQueue<StreamsDatum>();
        start();
    }


    protected void flushIfNecessary() {
        long lastLatency = System.currentTimeMillis() - lastWrite.get();
        //Flush iff the size > 0 AND the size is divisible by 100 or the time between now and the last flush is greater
        //than the maximum desired latency
        if (insertBatch.size() > 0 && (insertBatch.size() % BATCH_SIZE == 0 || lastLatency > MAX_WRITE_LATENCY)) {
            try {
                flush();
            } catch (IOException e) {
                LOGGER.error("Error writing to Mongo", e);
            }
        }
    }

    protected void addToBatch(T object) {
        try {
            lock.readLock().lock();
            insertBatch.add(object);
        } finally {
            lock.readLock().unlock();
        }
    }

    protected void connect() {};

    protected abstract T prepareObject(StreamsDatum streamsDatum);

    public abstract void writeList(List<T> insertBatch) throws IOException;

    public void disconnect() throws IOException {}
}

