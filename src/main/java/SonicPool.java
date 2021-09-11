import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class SonicPool {
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

    public synchronized <T extends SonicClient> T saveAndGetClient(Map<Long, T> pool, Supplier<T> client) {
        long curThreadId = Thread.currentThread().getId();
        if (pool.containsKey(curThreadId) && !pool.get(curThreadId).isClosed()) {
            return pool.get(curThreadId);
        }
        T gotClient = client.get();
        pool.put(curThreadId, gotClient);
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
