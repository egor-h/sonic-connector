package com.github.egorh.sonic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SonicClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(SonicClient.class);
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
        log.trace("Open socket {}:{} timeout: {}", this.host, this.port, this.timeout);
        try {
            sonicSocket = new Socket(this.host, this.port);
            sonicSocket.setSoTimeout(timeout);
            sonicInput = new BufferedReader(new InputStreamReader(sonicSocket.getInputStream()));
            sonicOutput = new BufferedWriter(new OutputStreamWriter(sonicSocket.getOutputStream()));
            checkReply(sonicInput.readLine(), r -> r.startsWith("CONNECTED"));
            start();
            this.closed = false;
        } catch (IOException e) {
            log.error("Exception during opening socket", e);
            throw new SonicException(e);
        }
    }

    protected String sendCommand(String command, String ...args) {
        if (threadId != Thread.currentThread().getId()) {
            log.error("Foreign thread invocation");
            throw new SonicException("Current version supports single connection per thread. Create more connection instances for each thread.");
        }

        try {
            if (command.equals("START")) {
                log.trace("skip check on START command");
            } else {
                if (!pingUnchecked()) {
                    log.trace("Socket is closed. Open new one.."); // socket.isClosed() always returns true so recreate socket on null response
                    openSocket();
                }
            }
            String preparedCommand = command + " " + Arrays.stream(args).collect(Collectors.joining(" ")).trim() + "\r\n";
            log.trace("sendCommand {}", preparedCommand);
            sonicOutput.write(preparedCommand);
            sonicOutput.flush();
            String reply = sonicInput.readLine();
            log.trace("reply: {}", reply);
            return reply;
        } catch (IOException e) {
            if (!sonicSocket.isClosed()) {
                try {
                    this.sonicSocket.close();
                } catch (IOException e2) {
                    throw new SonicException(e2);
                }
            }
            closed = true;
            throw new SonicException(e);
        }
    }

    protected String readLine() {
        try {
            String reply = sonicInput.readLine();
            log.trace("readline {}", reply);
            return reply;
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

    protected boolean pingUnchecked() {
        try {
            sonicOutput.write("PING\r\n ");
            sonicOutput.flush();
            String reply = sonicInput.readLine();
            log.trace("pingUnchecked reply: {}", reply);
            return reply != null && reply.startsWith("PONG");
        } catch (IOException e) {
            throw new SonicException(e);
        }

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

    protected String escape(String s) {
        return s.replace("\n", " ").replace("\r", "").trim();
    }

}
