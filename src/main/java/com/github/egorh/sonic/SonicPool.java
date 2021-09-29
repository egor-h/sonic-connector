package com.github.egorh.sonic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class SonicPool {
    private static Logger log = LoggerFactory.getLogger(SonicPool.class);
    private static Map<Long, ControlClient> CONTROL_POOL = new HashMap<>();
    private static Map<Long, IngestClient> INGEST_POOL = new HashMap<>();
    private static Map<Long, SearchClient> SEARCH_POOL = new HashMap<>();

    private String address;
    private int port;
    private String password;
    private int timeout;

    public SonicPool(String address, int port, String password, int timeout) {
        this.address = address;
        this.port = port;
        this.password = password;
        this.timeout = timeout;
    }

    protected synchronized <T extends SonicClient> T saveAndGetClient(Map<Long, T> pool, Supplier<T> client) {
        long curThreadId = Thread.currentThread().getId();
        log.trace("saveAndGetClient invoke on thread {}", curThreadId);
        if (pool.containsKey(curThreadId) && !pool.get(curThreadId).isClosed()) {
            log.trace("has non closed connection for thread");
            return pool.get(curThreadId);
        }
        T gotClient = client.get();
        pool.put(curThreadId, gotClient);
        log.trace("no saved connection for thread, save and return {}", gotClient.getClass().getName());
        return gotClient;
    }

    public ControlClient controlClient() {
        return saveAndGetClient(CONTROL_POOL, () -> new ControlClient(address, port, password, timeout));
    }

    public IngestClient ingestClient() {
        return saveAndGetClient(INGEST_POOL, () -> new IngestClient(address, port, password, timeout));
    }

    public SearchClient searchClient() {
        return saveAndGetClient(SEARCH_POOL, () -> new SearchClient(address, port, password, timeout));
    }
}
