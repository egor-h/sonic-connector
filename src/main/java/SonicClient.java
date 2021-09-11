import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SonicClient implements Closeable {
    public static final int SONIC_DEFAULT_PORT = 1491;
    public static final int SONIC_DEFAULT_TIMEOUT = 20000;

    protected static final Predicate<String> PRED_HAS_OK = r -> r.startsWith("OK");

    private long threadId = Thread.currentThread().getId();

    private String host;
    private int port;
    private String password;
    private int timeout;

    private Socket sonicSocket;
    private BufferedReader sonicInput;
    private BufferedWriter sonicOutput;

    private boolean closed;

    public SonicClient(String host, int port, String password, int timeout) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.timeout = timeout;
        openSocket();
    }

    public SonicClient(String host, int port, String password) {
        this(host, port, password, SONIC_DEFAULT_TIMEOUT);
    }

    public SonicClient(String host, String password) {
        this(host, SONIC_DEFAULT_PORT, password);
    }

    protected void openSocket() {
        try {
            sonicSocket = new Socket(this.host, this.port);
            sonicSocket.setSoTimeout(timeout);
            sonicInput = new BufferedReader(new InputStreamReader(sonicSocket.getInputStream()));
            sonicOutput = new BufferedWriter(new OutputStreamWriter(sonicSocket.getOutputStream()));
            checkReply(sonicInput.readLine(), r -> r.startsWith("CONNECTED"));
            start();
            this.closed = false;
        } catch (IOException e) {
            throw new SonicException(e);
        }
    }

    protected String sendCommand(String command, String ...args) {
        if (threadId != Thread.currentThread().getId()) {
            throw new SonicException("Current version supports single connection per thread. Create more connection instances for each thread.");
        }
        try {
            sonicOutput.write(command + " " + Arrays.stream(args).collect(Collectors.joining(" ")) + "\r\n");
            sonicOutput.flush();
            return sonicInput.readLine();
        } catch (IOException e) {
            throw new SonicException(e);
        }
    }

    protected String readLine() {
        try {
            return sonicInput.readLine();
        } catch (IOException e) {
            throw new SonicException(e);
        }
    }

    public String start(ChannelMode mode) {
        return checkReply(sendCommand("START", mode.name().toLowerCase(), password), r -> r.startsWith("STARTED"));
    }

    public String start() {
        return checkReply(sendCommand("START", ChannelMode.UNINITIALIZED.name().toLowerCase(), password), r -> r.startsWith("STARTED"));
    }

    public String quit() {
        return checkReply(sendCommand("QUIT"), r -> r.startsWith("ENDED"));
    }

    public String checkReply(String reply, Predicate<String> predicate) {
        if (reply.startsWith("ERR")) {
            throw new SonicException("Server returned error: " + reply);
        }
        if (predicate.test(reply)) {
            return reply;
        }
        throw new SonicException("Unexpected reply " + reply);
    }

    public String ping() {
        return checkReply(sendCommand("PING"), r -> r.startsWith("PONG"));
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        try {
            quit();
        } finally {
            try {
                if (!sonicSocket.isClosed()) {
                    sonicSocket.close();
                }
            } catch (IOException e) {
                throw new SonicException(e);
            }

            this.closed = true;
        }
    }

    protected String wrap(String s) {
        return String.format("\"%s\"", s);
    }

    public static void main(String[] args) {
        try (IngestClient ingestClient = new IngestClient("localhost", 14910, "SecretPassword");
             ControlClient controlClient = new ControlClient("localhost", 14910, "SecretPassword");
             SearchClient searchClient = new SearchClient("localhost", 14910, "SecretPassword")) {
//            System.out.println(ingestMode.start());
//            System.out.println(controlMode.start());
            System.out.println(ingestClient.ping());
            System.out.println(ingestClient.push("col1", "buck1", "1", "Some text about nothing"));
            System.out.println(ingestClient.push("col2", "buck1", "2", "Some text about nothing"));
            System.out.println(ingestClient.push("col1", "buck1", "3", "Some text about something"));
            System.out.println(ingestClient.push("col1", "buck1", "4", "Some text about nothingasd"));
            System.out.println(ingestClient.push("col1", "buck1", "5", "Some text about somethingasd"));
            controlClient.consolidate();
            System.out.println(ingestClient.count("col1",  "buck1", "2"));
            System.out.println(ingestClient.count("col1",  "buck1", "1"));
            System.out.println(ingestClient.count("col1",  "buck1"));
            System.out.println(ingestClient.count("col1"));

            new Thread(() -> searchClient.query("col1", "buck1", "some")).start();

//            System.out.println(searchMode.start());
            System.out.println(searchClient.query("col1", "buck1", "some"));
            System.out.println(searchClient.query("col1", "buck1", "noth"));
            System.out.println(searchClient.query("col2", "buck1", "noth"));
            System.out.println(searchClient.query("col1", "buck1", "never"));

        }

    }
}
