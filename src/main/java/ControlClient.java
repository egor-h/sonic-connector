public class ControlClient extends SonicClient {
    public enum TriggerAction {
        CONSOLIDATE, BACKUP, RESTORE
    }

    public ControlClient(String host, int port, String password, int timeout) {
        super(host, port, password, timeout);
    }

    public ControlClient(String host, int port, String password) {
        super(host, port, password);
    }

    public ControlClient(String host, String password) {
        super(host, password);
    }

    @Override
    public String start() {
        return start(ChannelMode.CONTROL);
    }

    public String trigger(TriggerAction action, String ...args) {
        String[] appendArgs = new String[args.length+1];
        appendArgs[0] = action.name().toLowerCase();
        System.arraycopy(args, 0, appendArgs, 1, args.length);
        return sendCommand("TRIGGER", appendArgs);
    }

    public String consolidate() {
        return checkReply(trigger(TriggerAction.CONSOLIDATE), r -> r.startsWith("OK"));
    }
}
