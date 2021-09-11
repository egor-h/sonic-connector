import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SearchClient extends SonicClient {
    public SearchClient(String host, int port, String password, int timeout) {
        super(host, port, password, timeout);
    }

    public SearchClient(String host, int port, String password) {
        super(host, port, password);
    }

    public SearchClient(String host, String password) {
        super(host, password);
    }

    @Override
    public String start() {
        return start(ChannelMode.SEARCH);
    }

    public List<String> query(String collection, String bucket, String term, long count, long offset, String locale) {
        String countStr = count == -1 ? "" : String.valueOf(count);
        String offsetStr = offset == -1 ? "" : String.valueOf(offset);
        String localeStr = locale.isEmpty() ? "" : String.format("LANG(%s)");
        String reply = checkReply(sendCommand("QUERY", collection, bucket, wrap(term), countStr, offsetStr, localeStr), r -> r.startsWith("PENDING"));
        String pendingId = reply.split(" ")[1];
        String nextLine = super.readLine();
        checkReply(nextLine, r -> nextLine.startsWith("EVENT QUERY") && nextLine.contains(pendingId));
        return Arrays.stream(nextLine.split(" ")).skip(3).collect(Collectors.toList());
    }

    public List<String> query(String collection, String bucket, String term, long count, long offset) {
        return query(collection, bucket, term, count, offset, "");
    }

    public List<String> query(String collection, String bucket, String term, long count) {
        return query(collection, bucket, term, count, -1);
    }

    public List<String> query(String collection, String bucket, String term) {
        return query(collection, bucket, term, -1);
    }

    public List<String> query(String collection, String bucket, List<String> terms) {
        return query(collection, bucket, terms.stream().collect(Collectors.joining(" ")));
    }

    public List<String> query(String collection, String bucket, List<String> terms, long count) {
        return query(collection, bucket, terms.stream().collect(Collectors.joining(" ")), count);
    }

    public List<String> suggest(String collection, String bucket, String word, long limit) {
        String limitStr = limit == -1 ? "" : String.format("LIMIT(%s)", limit);
        String reply = checkReply(sendCommand("SUGGEST", collection, bucket, wrap(word), limitStr), r -> r.startsWith("PENDING"));
        String pendingId = reply.split(" ")[1];
        String nextLine = super.readLine();
        checkReply(nextLine, r -> nextLine.startsWith("EVENT QUERY") && nextLine.contains(pendingId));
        return Arrays.stream(nextLine.split(" ")).skip(3).collect(Collectors.toList());
    }

    public List<String> suggest(String collection, String bucket, String word) {
        return suggest(collection, bucket, word, -1);
    }
}
