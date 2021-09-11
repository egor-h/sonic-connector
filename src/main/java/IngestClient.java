import java.util.function.Predicate;

public class IngestClient extends SonicClient {
    private static Predicate<String> RESULT_CHECKER = r -> r.startsWith("RESULT") && r.split(" ").length == 2;

    public IngestClient(String host, int port, String password, int timeout) {
        super(host, port, password, timeout);
    }

    public IngestClient(String host, int port, String password) {
        super(host, port, password);
    }

    public IngestClient(String host, String password) {
        super(host, password);
    }

    public String start() {
        return start(ChannelMode.INGEST);
    }

    public String push(String collection, String bucket, String object, String data, String locale) {
        return checkReply(sendCommand("PUSH", collection, bucket, object, wrap(escape(data)), locale.isEmpty() ? "" : String.format("LANG(%s)", locale)), PRED_HAS_OK);
    }

    public String push(String collection, String bucket, String object, String data) {
        return push(collection, bucket, object, data, "");
    }

    public String pop(String collection, String bucket, String object, String data) {
        return checkReply(sendCommand("POP", collection, bucket, object, wrap(data)), PRED_HAS_OK);
    }

    public int count(String collection, String bucket, String object) {
        String countResult = checkReply(sendCommand("COUNT", collection, bucket, object), RESULT_CHECKER);
        return Integer.parseInt(countResult.split(" ")[1]);
    }

    public int count(String collection, String bucket) {
        String countResult = checkReply(sendCommand("COUNT", collection, bucket), RESULT_CHECKER);
        return Integer.parseInt(countResult.split(" ")[1]);
    }

    public int count(String collection) {
        String countResult = checkReply(sendCommand("COUNT", collection), RESULT_CHECKER);
        return Integer.parseInt(countResult.split(" ")[1]);
    }

    public int flushCollection(String collection) {
        String flushResult = checkReply(sendCommand("FLUSHC", collection), RESULT_CHECKER);
        return Integer.parseInt(flushResult.split(" ")[1]);
    }

    public int flushBucket(String collection, String bucket) {
        String flushResult = checkReply(sendCommand("FLUSHB", collection, bucket), RESULT_CHECKER);
        return Integer.parseInt(flushResult.split(" ")[1]);
    }

    public int flushObject(String collection, String bucket, String object) {
        String flushResult = checkReply(sendCommand("FLUSHO", collection, bucket, object), RESULT_CHECKER);
        return Integer.parseInt(flushResult.split(" ")[1]);
    }
}
