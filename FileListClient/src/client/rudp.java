package client;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class rudp {

    // Protocol constants
    private static final int HEADER_SIZE = 10;      // 1 byte type, 1 byte file_id, 4 start, 4 end
    private static final int MAX_DATA_SIZE = 1000;  // as specified
    private static final int MAX_PACKET_SIZE = HEADER_SIZE + MAX_DATA_SIZE;

    // Request types
    private static final int REQ_GET_FILE_LIST = 1;
    private static final int REQ_GET_FILE_SIZE = 2;
    private static final int REQ_GET_FILE_DATA = 3;

    // Error response types
    private static final int RESP_INVALID_REQUEST_TYPE = 100;
    private static final int RESP_INVALID_FILE_ID = 101;
    private static final int RESP_INVALID_START_OR_END = 102;

    private enum ChunkStatus { NOT_REQUESTED, IN_FLIGHT, RECEIVED }

    // Retries per chunk (covers multiple sub-range requests too)
    private static final int MAX_CHUNK_RETRIES = 35;

    // Path health knobs
    private static final int PATH_CONSEC_TIMEOUT_DISABLE_THRESHOLD = 120;     
    private static final long PATH_DISABLE_COOLDOWN_NS = 1_000_000_000L;      // 1s cooldown
    private static final long PATH_REQUIRE_NO_ACK_NS = 900_000_000L;          // must have had no ACK for 0.9s to disable

    // Probing while disabled (prevents total stalls)
    private static final long DISABLED_PROBE_INTERVAL_NS = 200_000_000L;      // 200ms
    private static final int DISABLED_PROBE_MAX_INFLIGHT = 1;                

    // Pacing token bucket (per path)
    // Your logs show the server/network behaves poorly under high request bursts; keep this moderate.
    private static final double PACING_RATE_REQ_PER_SEC = 600.0;             // per path
    private static final double PACING_BURST_TOKENS = 120.0;                 // short bursts allowed

    // Window bounds
    private static final int INITIAL_WINDOW = 12;
    private static final int MIN_WINDOW = 2;
    private static final int MAX_WINDOW = 96;

    // Timeout clamps (ms)
    private static final double RTO_MIN_MS = 250.0;
    private static final double RTO_MAX_MS = 3000.0;

    // Represents a file chunk [start,end] inclusive in 1-based indexing.
    private static class Chunk {
        final int index;
        final long start; // 1-based inclusive
        final long end;   // 1-based inclusive
        final int len;    // bytes in this chunk (<=1000)

        volatile ChunkStatus status = ChunkStatus.NOT_REQUESTED;

        // which path it was last sent on (for inFlight accounting)
        volatile int pathIndex = -1;
        volatile long lastSentTimeNs = 0L;
        volatile int retries = 0;

        // exponential backoff gating for resends
        volatile long nextEligibleSendNs = 0L;

        // queue dedup flag
        volatile boolean enqueued = false;

        // per-byte received tracking for correctness (len <= 1000)
        final boolean[] received;
        int receivedCount = 0;

        Chunk(int index, long start, long end) {
            this.index = index;
            this.start = start;
            this.end = end;
            this.len = (int) (end - start + 1);
            this.received = new boolean[this.len];
        }

        boolean isComplete() {
            return receivedCount >= len;
        }

        void onTimeoutBackoff(long nowNs) {
            // base 40ms, exponential up to ~2s
            int exp = Math.min(6, retries); // 2^6 = 64
            long backoffMs = 40L * (1L << exp);
            backoffMs = Math.min(2000L, backoffMs);
            nextEligibleSendNs = nowNs + backoffMs * 1_000_000L;
        }

        int[] nextMissingRange() {
            if (isComplete()) return null;

            int i = -1;
            for (int k = 0; k < len; k++) {
                if (!received[k]) { i = k; break; }
            }
            if (i < 0) return null;

            int j = i;
            int maxLen = Math.min(MAX_DATA_SIZE, len - i);
            while (j < len && (j - i) < maxLen && !received[j]) {
                j++;
            }
            // [i, j) are missing and contiguous false
            int startByte = (int) (start + i);
            int endByte = (int) (start + (j - 1));
            return new int[]{ startByte, endByte };
        }
    }

    private static class PathState {
        final InetAddress serverAddress;
        final int serverPort;
        final DatagramSocket socket;
        final InetAddress boundLocalAddress; // may be null (unbound / OS-chosen)

        // congestion-ish
        volatile int windowSize = INITIAL_WINDOW;
        volatile int inFlight = 0;

        // delivery stats
        volatile long bytesUniqueDelivered = 0; // unique bytes that filled holes
        volatile long totalDataBytes = 0;       // all payload bytes received (duplicates included)
        volatile int sentRequests = 0;          // how many REQ_GET_FILE_DATA were sent
        volatile int ackedRequests = 0;         // how many in-flight requests got a response
        volatile int timeouts = 0;

        // RTO estimator (RFC6298-ish)
        volatile boolean rttInitialized = false;
        volatile double srttMs = 120.0;
        volatile double rttvarMs = 60.0;
        volatile double rtoMs = 500.0;

        // path health
        volatile int consecutiveTimeouts = 0;
        volatile long disabledUntilNs = 0L;
        volatile long lastAckNs = System.nanoTime();
        volatile long lastProbeSentNs = 0L;

        // pacing token bucket
        volatile double tokens = PACING_BURST_TOKENS;
        volatile long lastTokenUpdateNs = System.nanoTime();

        PathState(InetAddress addr, int port, InetAddress bindLocalAddr) throws SocketException {
            this.serverAddress = addr;
            this.serverPort = port;
            this.boundLocalAddress = bindLocalAddr;

            // Create unbound socket, then bind explicitly if requested.
            DatagramSocket s = new DatagramSocket(null);
            s.setReuseAddress(true);

            try {
                if (bindLocalAddr != null) {
                    // bind to specific local interface IP, ephemeral port
                    s.bind(new InetSocketAddress(bindLocalAddr, 0));
                } else {
                    // default: bind to wildcard, ephemeral port
                    s.bind(new InetSocketAddress(0));
                }
            } catch (SocketException e) {
                // If binding fails, fall back to default behavior to avoid breaking runs.
                try {
                    s.close();
                } catch (Exception ignored) {}
                DatagramSocket fallback = new DatagramSocket();
                this.socket = fallback;
                fallback.setReceiveBufferSize(4 * 1024 * 1024);
                fallback.setSendBufferSize(1 * 1024 * 1024);
                fallback.setSoTimeout(0);
                return;
            }

            this.socket = s;
            this.socket.setReceiveBufferSize(4 * 1024 * 1024);
            this.socket.setSendBufferSize(1 * 1024 * 1024);
            this.socket.setSoTimeout(0);
        }

        void close() { socket.close(); }

        double getRtoMs() {
            double v = rtoMs;
            if (v < RTO_MIN_MS) v = RTO_MIN_MS;
            if (v > RTO_MAX_MS) v = RTO_MAX_MS;
            return v;
        }

        void updateRtt(double sampleMs) {
            if (sampleMs <= 0 || sampleMs > 10_000) return;

            final double alpha = 0.125;
            final double beta = 0.25;

            if (!rttInitialized) {
                srttMs = sampleMs;
                rttvarMs = sampleMs / 2.0;
                rttInitialized = true;
            } else {
                double err = Math.abs(srttMs - sampleMs);
                rttvarMs = (1.0 - beta) * rttvarMs + beta * err;
                srttMs = (1.0 - alpha) * srttMs + alpha * sampleMs;
            }
            rtoMs = srttMs + 4.0 * rttvarMs;

            if (rtoMs < RTO_MIN_MS) rtoMs = RTO_MIN_MS;
            if (rtoMs > RTO_MAX_MS) rtoMs = RTO_MAX_MS;
        }

        boolean isDisabled(long nowNs) {
            return nowNs < disabledUntilNs;
        }

        void disableForCooldown(long nowNs) {
            disabledUntilNs = nowNs + PATH_DISABLE_COOLDOWN_NS;
        }

        boolean tryConsumeToken(long nowNs) {
            long dtNs = nowNs - lastTokenUpdateNs;
            if (dtNs > 0) {
                double dtSec = dtNs / 1e9;
                tokens += dtSec * PACING_RATE_REQ_PER_SEC;
                if (tokens > PACING_BURST_TOKENS) tokens = PACING_BURST_TOKENS;
                lastTokenUpdateNs = nowNs;
            }
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            return false;
        }

        boolean shouldAllowProbe(long nowNs) {
            if (!isDisabled(nowNs)) return true;
            if (inFlight > DISABLED_PROBE_MAX_INFLIGHT) return false;
            return (nowNs - lastProbeSentNs) >= DISABLED_PROBE_INTERVAL_NS;
        }
    }

    private static class FileEntry {
        final int id;
        final String name;
        FileEntry(int id, String name) { this.id = id; this.name = name; }
        @Override public String toString() { return id + "\t" + name; }
    }

    private static class Endpoint {
        final String host;
        final int port;
        Endpoint(String host, int port) { this.host = host; this.port = port; }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java client.rudp server_IP1:port1 server_IP2:port2");
            System.exit(1);
        }

        try {
            Endpoint ep1 = parseEndpoint(args[0]);
            Endpoint ep2 = parseEndpoint(args[1]);

            InetAddress serverIP1 = InetAddress.getByName(ep1.host);
            InetAddress serverIP2 = InetAddress.getByName(ep2.host);

            DatagramSocket controlSocket = new DatagramSocket();
            controlSocket.setSoTimeout(1000);

            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.println("=== Reliable Multi-Path UDP Client ===");

                List<FileEntry> fileList = getFileList(controlSocket, serverIP1, ep1.port);
                if (fileList.isEmpty()) {
                    System.out.println("No files available on server.");
                    break;
                }

                System.out.println("File List:");
                for (FileEntry fe : fileList) System.out.println(fe);

                System.out.print("Enter a file ID to download (or 0 to exit): ");
                int fileId;
                try {
                    fileId = Integer.parseInt(scanner.nextLine().trim());
                } catch (Exception e) {
                    System.out.println("Invalid input. Exiting.");
                    break;
                }
                if (fileId == 0) {
                    System.out.println("Exiting.");
                    break;
                }

                long fileSize = getFileSize(controlSocket, serverIP1, ep1.port, (byte) fileId);
                if (fileSize <= 0) {
                    System.out.println("Invalid file size returned. Aborting.");
                    continue;
                }
                if (fileSize > Integer.MAX_VALUE) {
                    System.out.println("File too large for this implementation.");
                    continue;
                }

                System.out.println("File " + fileId + " size: " + fileSize + " bytes.");
                System.out.println("Starting download over two paths...");

                downloadFile(serverIP1, ep1.port, serverIP2, ep2.port, fileId, fileSize);

                System.out.println("\nReturning to main menu.\n");
            }

            controlSocket.close();
            scanner.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Endpoint parseEndpoint(String arg) {
        String[] parts = arg.split(":");
        if (parts.length != 2) throw new IllegalArgumentException("Endpoint must be host:port, got: " + arg);
        return new Endpoint(parts[0], Integer.parseInt(parts[1]));
    }

    // ===== Control plane =====

    private static List<FileEntry> getFileList(DatagramSocket socket, InetAddress serverIP, int serverPort)
            throws IOException {
        byte[] req = encodeRequest((byte) REQ_GET_FILE_LIST, (byte) 0, 0, 0);
        byte[] respBuf = new byte[MAX_PACKET_SIZE];
        DatagramPacket resp = sendWithRetry(socket, serverIP, serverPort, req, respBuf, 5, 1000);
        return decodeFileList(resp.getData(), resp.getLength());
    }

    private static long getFileSize(DatagramSocket socket, InetAddress serverIP, int serverPort, byte fileId)
            throws IOException {
        byte[] req = encodeRequest((byte) REQ_GET_FILE_SIZE, fileId, 0, 0);
        byte[] respBuf = new byte[MAX_PACKET_SIZE];
        DatagramPacket resp = sendWithRetry(socket, serverIP, serverPort, req, respBuf, 5, 1000);
        return decodeFileSize(resp.getData(), resp.getLength(), fileId);
    }

    private static DatagramPacket sendWithRetry(DatagramSocket socket,
                                               InetAddress serverIP,
                                               int serverPort,
                                               byte[] req,
                                               byte[] respBuf,
                                               int maxRetries,
                                               int timeoutMs) throws IOException {
        socket.setSoTimeout(timeoutMs);

        DatagramPacket reqPacket = new DatagramPacket(req, req.length, serverIP, serverPort);
        DatagramPacket respPacket = new DatagramPacket(respBuf, respBuf.length);

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            socket.send(reqPacket);
            try {
                respPacket.setLength(respBuf.length);
                socket.receive(respPacket);
                return respPacket;
            } catch (SocketTimeoutException e) {
                System.out.println("Timeout waiting for response (attempt " + (attempt + 1) + ")...");
            }
        }

        throw new IOException("Failed to receive response after " + maxRetries + " attempts");
    }

    private static List<FileEntry> decodeFileList(byte[] data, int length) {
        List<FileEntry> files = new ArrayList<>();
        if (length < HEADER_SIZE) return files;

        int responseType = data[0] & 0xFF;
        if (responseType >= RESP_INVALID_REQUEST_TYPE) return files;
        if (responseType != REQ_GET_FILE_LIST) return files;

        int fileCount = data[1] & 0xFF;
        int offset = HEADER_SIZE;

        for (int i = 0; i < fileCount && offset < length; i++) {
            int fileId = data[offset] & 0xFF;
            offset += 1;
            StringBuilder name = new StringBuilder();
            while (offset < length && data[offset] != 0) {
                name.append((char) data[offset]);
                offset++;
            }
            if (offset < length && data[offset] == 0) offset++;
            files.add(new FileEntry(fileId, name.toString()));
        }

        return files;
    }

    private static long decodeFileSize(byte[] data, int length, byte expectedFileId) {
        if (length < HEADER_SIZE + 4) return -1;

        int responseType = data[0] & 0xFF;
        if (responseType >= RESP_INVALID_REQUEST_TYPE) return -1;
        if (responseType != REQ_GET_FILE_SIZE) return -1;

        int fileId = data[1] & 0xFF;
        if ((byte) fileId != expectedFileId) return -1;

        ByteBuffer bb = ByteBuffer.wrap(data, HEADER_SIZE, 4).order(ByteOrder.BIG_ENDIAN);
        return Integer.toUnsignedLong(bb.getInt());
    }

    // ===== Queue helper =====
    private static void enqueueIfNeeded(Chunk c, ConcurrentLinkedDeque<Integer> pending) {
        synchronized (c) {
            if (!c.enqueued && c.status == ChunkStatus.NOT_REQUESTED && !c.isComplete()) {
                pending.addLast(c.index);
                c.enqueued = true;
            }
        }
    }

    // ===== Two-interface binding helpers =====

    private static InetAddress getOptionalBindAddress(int pathIdx /* 1-based */) {
        // system property wins over env var
        String propKey = "rudp.bind" + pathIdx;
        String envKey = "RUDP_BIND" + pathIdx;

        String v = System.getProperty(propKey);
        if (v == null || v.trim().isEmpty()) v = System.getenv(envKey);
        if (v == null || v.trim().isEmpty()) return null;

        v = v.trim();
        try {
            InetAddress a = InetAddress.getByName(v);

            NetworkInterface nif = NetworkInterface.getByInetAddress(a);
            if (nif == null) {
                System.out.println("[Bind] WARNING: " + v + " is not assigned to any local interface. Ignoring bind for path " + pathIdx + ".");
                return null;
            }
            if (!nif.isUp()) {
                System.out.println("[Bind] WARNING: interface for " + v + " is DOWN. Ignoring bind for path " + pathIdx + ".");
                return null;
            }
            // Allow loopback if user explicitly chooses it (e.g., docker local tests)
            return a;
        } catch (Exception e) {
            System.out.println("[Bind] WARNING: failed to parse/validate bind IP '" + v + "' for path " + pathIdx + ". Ignoring.");
            return null;
        }
    }

    private static String localBindLabel(InetAddress a) {
        return (a == null) ? "OS default (not pinned to NIC)" : a.getHostAddress();
    }

    // ===== Data plane =====

    private static void downloadFile(InetAddress serverIP1, int port1,
                                     InetAddress serverIP2, int port2,
                                     int fileId, long fileSize) throws Exception {

        byte[] fileData = new byte[(int) fileSize];

        int numChunks = (int) ((fileSize + MAX_DATA_SIZE - 1) / MAX_DATA_SIZE);
        Chunk[] chunks = new Chunk[numChunks];
        for (int i = 0; i < numChunks; i++) {
            long start = (long) i * MAX_DATA_SIZE + 1;
            long end = Math.min(fileSize, start + MAX_DATA_SIZE - 1);
            chunks[i] = new Chunk(i, start, end);
        }

        AtomicInteger remainingChunks = new AtomicInteger(numChunks);
        AtomicLong uniqueBytesReceived = new AtomicLong(0);
        AtomicBoolean downloadDone = new AtomicBoolean(false);
        AtomicBoolean aborted = new AtomicBoolean(false);

        // --- Force each path onto a specific local interface IP if configured ---
        InetAddress bind1 = getOptionalBindAddress(1);
        InetAddress bind2 = getOptionalBindAddress(2);

        PathState[] paths = new PathState[2];
        paths[0] = new PathState(serverIP1, port1, bind1);
        paths[1] = new PathState(serverIP2, port2, bind2);

        System.out.println("[Bind] Path 1 local bind: " + localBindLabel(bind1) + " | remote: " + serverIP1.getHostAddress() + ":" + port1);
        System.out.println("[Bind] Path 2 local bind: " + localBindLabel(bind2) + " | remote: " + serverIP2.getHostAddress() + ":" + port2);

        ConcurrentLinkedDeque<Integer> pending = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < numChunks; i++) {
            chunks[i].enqueued = true;
            pending.add(i);
        }

        Thread r1 = new Thread(new Receiver(0, paths[0], chunks, fileData, remainingChunks, uniqueBytesReceived, downloadDone, aborted, fileId, pending));
        Thread r2 = new Thread(new Receiver(1, paths[1], chunks, fileData, remainingChunks, uniqueBytesReceived, downloadDone, aborted, fileId, pending));
        r1.start();
        r2.start();

        long startTimeNs = System.nanoTime();
        long lastProgressNs = 0;
        long lastTimeoutSweepNs = 0;

        while (!downloadDone.get()) {
            long nowNs = System.nanoTime();

            // ===== Timeout sweep =====
            if (nowNs - lastTimeoutSweepNs > 25_000_000L) { // every 25ms
                for (Chunk c : chunks) {
                    int pi = c.pathIndex;
                    if (c.status == ChunkStatus.IN_FLIGHT && pi >= 0) {
                        PathState p = paths[pi];
                        double rtoMs;
                        synchronized (p) {
                            rtoMs = p.getRtoMs();
                        }
                        long timeoutNs = (long) (rtoMs * 1_000_000.0);

                        if (nowNs - c.lastSentTimeNs > timeoutNs) {
                            boolean requeue = false;

                            synchronized (c) {
                                if (c.status == ChunkStatus.IN_FLIGHT) {
                                    c.status = ChunkStatus.NOT_REQUESTED;
                                    c.pathIndex = -1;
                                    c.lastSentTimeNs = 0;
                                    c.retries++;
                                    c.onTimeoutBackoff(nowNs);
                                    requeue = true;
                                }
                            }

                            synchronized (p) {
                                p.inFlight = Math.max(0, p.inFlight - 1);
                                p.timeouts++;
                                p.consecutiveTimeouts++;

                                // stronger multiplicative decrease on timeout
                                if (p.windowSize > MIN_WINDOW) {
                                    p.windowSize = Math.max(MIN_WINDOW, (int) Math.ceil(p.windowSize * 0.55));
                                }

                                // disable only if it's really "dead": many timeouts AND no ACK recently
                                if (p.consecutiveTimeouts >= PATH_CONSEC_TIMEOUT_DISABLE_THRESHOLD &&
                                        (nowNs - p.lastAckNs) > PATH_REQUIRE_NO_ACK_NS) {
                                    p.disableForCooldown(nowNs);
                                    p.consecutiveTimeouts = 0;
                                }
                            }

                            if (c.retries >= MAX_CHUNK_RETRIES && !c.isComplete()) {
                                System.out.println("Chunk " + c.index + " exceeded max retries; aborting download.");
                                aborted.set(true);
                                downloadDone.set(true);
                                break;
                            } else if (requeue && !c.isComplete()) {
                                enqueueIfNeeded(c, pending);
                            }
                        }
                    }
                }
                lastTimeoutSweepNs = nowNs;
            }

            if (downloadDone.get()) break;

            // refill if empty but still missing
            if (pending.isEmpty() && remainingChunks.get() > 0) {
                for (Chunk c : chunks) {
                    enqueueIfNeeded(c, pending);
                }
            }

            // ===== Scheduler =====
            for (int pi = 0; pi < 2; pi++) {
                PathState p = paths[pi];

                while (true) {
                    int window, inFlight;
                    boolean allowedToSend;
                    boolean allowProbe;

                    synchronized (p) {
                        window = p.windowSize;
                        inFlight = p.inFlight;

                        // allow probing even when disabled, but keep it tiny
                        allowProbe = p.shouldAllowProbe(nowNs);
                        if (!allowProbe) break;

                        int effectiveWindow = p.isDisabled(nowNs) ? Math.min(DISABLED_PROBE_MAX_INFLIGHT, window) : window;
                        if (inFlight >= effectiveWindow) break;

                        allowedToSend = p.tryConsumeToken(nowNs);
                    }
                    if (!allowedToSend) break;

                    Integer idx = pending.poll();
                    if (idx == null) break;

                    Chunk c = chunks[idx];

                    synchronized (c) {
                        c.enqueued = false; // we popped it

                        if (c.status != ChunkStatus.NOT_REQUESTED) continue;
                        if (c.isComplete()) {
                            c.status = ChunkStatus.RECEIVED;
                            c.pathIndex = -1;
                            c.lastSentTimeNs = 0;
                            continue;
                        }
                        if (nowNs < c.nextEligibleSendNs) {
                            // put back later
                            enqueueIfNeeded(c, pending);
                            continue;
                        }

                        int[] range = c.nextMissingRange();
                        if (range == null) {
                            c.status = ChunkStatus.RECEIVED;
                            continue;
                        }

                        int reqStart = range[0];
                        int reqEnd = range[1];

                        byte[] req = encodeRequest((byte) REQ_GET_FILE_DATA, (byte) fileId, reqStart, reqEnd);
                        DatagramPacket dp = new DatagramPacket(req, req.length, p.serverAddress, p.serverPort);

                        try {
                            p.socket.send(dp);
                        } catch (IOException e) {
                            enqueueIfNeeded(c, pending);
                            continue;
                        }

                        c.status = ChunkStatus.IN_FLIGHT;
                        c.pathIndex = pi;
                        c.lastSentTimeNs = nowNs;

                        synchronized (p) {
                            p.inFlight++;
                            p.sentRequests++;
                            if (p.isDisabled(nowNs)) {
                                p.lastProbeSentNs = nowNs;
                            }
                        }
                    }
                }
            }

            if (remainingChunks.get() <= 0) downloadDone.set(true);

            // ===== Progress =====
            if (nowNs - lastProgressNs > 1_000_000_000L) {
                double elapsedSec = (nowNs - startTimeNs) / 1e9;
                long unique = uniqueBytesReceived.get();
                double percent = (unique * 100.0) / Math.max(1.0, (double) fileSize);

                System.out.printf("\n[Progress] %.2f%% complete (%.2f / %.2f MB, %.2f s)\n",
                        percent,
                        unique / (1024.0 * 1024.0),
                        fileSize / (1024.0 * 1024.0),
                        elapsedSec);

                for (int pidx = 0; pidx < 2; pidx++) {
                    PathState ps = paths[pidx];
                    double goodputKBps = ps.bytesUniqueDelivered / 1024.0 / Math.max(0.001, elapsedSec);
                    double loss = 1.0 - ((double) ps.ackedRequests / Math.max(1.0, (double) ps.sentRequests));
                    boolean disabled = ps.isDisabled(System.nanoTime());
                    System.out.printf("Path %d: window=%d, inFlight=%d, SRTT=%.1f ms, RTO=%.1f ms, goodput=%.1f KB/s, loss=%.1f%%%s\n",
                            pidx + 1,
                            ps.windowSize,
                            ps.inFlight,
                            ps.srttMs,
                            ps.getRtoMs(),
                            goodputKBps,
                            loss * 100.0,
                            disabled ? " (DISABLED: probing)" : ""
                    );
                }

                lastProgressNs = nowNs;
            }

            Thread.sleep(1);
        }

        long endTimeNs = System.nanoTime();
        long elapsedMs = (endTimeNs - startTimeNs) / 1_000_000L;

        // stop receivers
        paths[0].close();
        paths[1].close();
        r1.join();
        r2.join();

        // Final completeness check
        boolean allComplete = (uniqueBytesReceived.get() == fileSize) && (remainingChunks.get() <= 0);
        if (allComplete) {
            for (Chunk c : chunks) {
                if (!c.isComplete()) { allComplete = false; break; }
            }
        }

        String md5 = computeMD5(fileData);

        long totalDataBytes = paths[0].totalDataBytes + paths[1].totalDataBytes;
        double ratio = (double) fileSize / Math.max(1L, totalDataBytes);

        System.out.println("\n===== Download Summary =====");
        System.out.println("File ID: " + fileId);
        System.out.println("File size: " + fileSize + " bytes");
        System.out.println("Elapsed time: " + elapsedMs + " ms");
        System.out.println("MD5: " + md5 + (allComplete ? "" : " (NOTE: download incomplete)"));
        System.out.println("Unique bytes received: " + uniqueBytesReceived.get());
        System.out.println("Total downloaded bytes (all paths): " + totalDataBytes);
        System.out.printf("File size / total downloaded bytes ratio: %.4f\n", ratio);

        double elapsedSec = (endTimeNs - startTimeNs) / 1e9;
        for (int pi = 0; pi < 2; pi++) {
            PathState p = paths[pi];
            double goodputKBps = p.bytesUniqueDelivered / 1024.0 / Math.max(0.001, elapsedSec);
            double loss = 1.0 - ((double) p.ackedRequests / Math.max(1.0, (double) p.sentRequests));
            System.out.printf("Path %d: sentRequests=%d, acked=%d, timeouts=%d, SRTT=%.1f ms, RTO=%.1f ms, goodput=%.1f KB/s, loss=%.1f%%\n",
                    pi + 1, p.sentRequests, p.ackedRequests, p.timeouts, p.srttMs, p.getRtoMs(), goodputKBps, loss * 100.0);
        }

        String outName;
        if (aborted.get() || !allComplete) {
            outName = "downloaded_file_" + fileId + ".partial";
            System.out.println("STATUS: INCOMPLETE (will NOT claim correct MD5). Wrote partial file.");
            if (aborted.get()) System.out.println("Reason: aborted due to max retries on at least one chunk.");
        } else {
            outName = "downloaded_file_" + fileId;
            System.out.println("STATUS: SUCCESS (all bytes received).");
        }

        try (FileOutputStream fos = new FileOutputStream(outName)) {
            fos.write(fileData);
        }
        System.out.println("File written to: " + outName);
    }

    private static class Receiver implements Runnable {
        private final int pathIndex;
        private final PathState path;
        private final Chunk[] chunks;
        private final byte[] fileBuffer;
        private final AtomicInteger remainingChunks;
        private final AtomicLong uniqueBytesReceived;
        private final AtomicBoolean downloadDone;
        private final AtomicBoolean aborted;
        private final int fileId;
        private final ConcurrentLinkedDeque<Integer> pending;

        Receiver(int pathIndex, PathState path, Chunk[] chunks, byte[] fileBuffer,
                 AtomicInteger remainingChunks, AtomicLong uniqueBytesReceived,
                 AtomicBoolean downloadDone, AtomicBoolean aborted, int fileId, ConcurrentLinkedDeque<Integer> pending) {
            this.pathIndex = pathIndex;
            this.path = path;
            this.chunks = chunks;
            this.fileBuffer = fileBuffer;
            this.remainingChunks = remainingChunks;
            this.uniqueBytesReceived = uniqueBytesReceived;
            this.downloadDone = downloadDone;
            this.aborted = aborted;
            this.fileId = fileId;
            this.pending = pending;
        }

        @Override
        public void run() {
            byte[] buf = new byte[MAX_PACKET_SIZE];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            while (!downloadDone.get()) {
                try {
                    packet.setLength(buf.length); // CRITICAL
                    path.socket.receive(packet);
                } catch (SocketException e) {
                    break; // socket closed
                } catch (IOException e) {
                    continue;
                }

                int len = packet.getLength();
                if (len < HEADER_SIZE) continue;

                byte[] data = packet.getData();
                int responseType = data[0] & 0xFF;

                // server error responses
                if (responseType >= RESP_INVALID_REQUEST_TYPE) {
                    int respFileId = data[1] & 0xFF;
                    if (respFileId != fileId) continue;

                    long startByte = 1;
                    try {
                        ByteBuffer bbe = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
                        bbe.position(2);
                        startByte = Integer.toUnsignedLong(bbe.getInt());
                    } catch (Exception ignored) {}

                    if (startByte <= 0) startByte = 1;
                    int chunkIndex = (int) ((startByte - 1) / MAX_DATA_SIZE);
                    if (chunkIndex < 0 || chunkIndex >= chunks.length) continue;

                    Chunk c = chunks[chunkIndex];
                    boolean wasInFlight;

                    synchronized (c) {
                        wasInFlight = (c.status == ChunkStatus.IN_FLIGHT && c.pathIndex == pathIndex);

                        if (!c.isComplete()) {
                            c.status = ChunkStatus.NOT_REQUESTED;
                            c.pathIndex = -1;
                            c.lastSentTimeNs = 0;
                            c.retries++;
                            c.onTimeoutBackoff(System.nanoTime());
                        } else {
                            c.status = ChunkStatus.RECEIVED;
                            c.pathIndex = -1;
                            c.lastSentTimeNs = 0;
                        }
                    }

                    if (wasInFlight) {
                        synchronized (path) {
                            path.inFlight = Math.max(0, path.inFlight - 1);
                            path.timeouts++;
                            path.consecutiveTimeouts++;
                        }
                    }

                    if (c.retries >= MAX_CHUNK_RETRIES && !c.isComplete()) {
                        System.out.println("Chunk " + c.index + " exceeded max retries due to server errors; aborting.");
                        aborted.set(true);
                        downloadDone.set(true);
                        return;
                    }

                    enqueueIfNeeded(c, pending);
                    continue;
                }

                if (responseType != REQ_GET_FILE_DATA) continue;

                int respFileId = data[1] & 0xFF;
                if (respFileId != fileId) continue;

                ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
                bb.position(2);
                long startByte = Integer.toUnsignedLong(bb.getInt());
                long endByte = Integer.toUnsignedLong(bb.getInt());
                if (startByte <= 0 || endByte < startByte) continue;

                int dataOffset = HEADER_SIZE;
                int dataLen = len - dataOffset;
                if (dataLen <= 0) continue;

                long rangeLenLong = (endByte - startByte + 1);
                if (rangeLenLong <= 0 || rangeLenLong > MAX_DATA_SIZE) continue;
                int rangeLen = (int) rangeLenLong;

                // Drop truncated datagrams
                if (dataLen < rangeLen) continue;

                int chunkIndex = (int) ((startByte - 1) / MAX_DATA_SIZE);
                if (chunkIndex < 0 || chunkIndex >= chunks.length) continue;

                Chunk c = chunks[chunkIndex];

                boolean wasInFlight;
                double sampleRttMs = -1.0;
                int newlyFilled = 0;
                boolean becameComplete = false;

                long nowNs = System.nanoTime();

                synchronized (c) {
                    wasInFlight = (c.status == ChunkStatus.IN_FLIGHT && c.pathIndex == pathIndex);

                    if (wasInFlight) {
                        long sent = c.lastSentTimeNs;
                        if (sent > 0) sampleRttMs = (nowNs - sent) / 1_000_000.0;

                        synchronized (path) {
                            path.inFlight = Math.max(0, path.inFlight - 1);
                            path.ackedRequests++;
                            path.consecutiveTimeouts = 0;
                            path.lastAckNs = nowNs;
                        }
                    }

                    if (c.isComplete()) {
                        c.status = ChunkStatus.RECEIVED;
                        c.pathIndex = -1;
                        c.lastSentTimeNs = 0;
                        continue;
                    }

                    // clamp response range to chunk
                    long safeStart = Math.max(c.start, startByte);
                    long safeEnd = Math.min(c.end, endByte);
                    if (safeStart > safeEnd) continue;

                    int safeLen = (int) (safeEnd - safeStart + 1);

                    int srcOffset = dataOffset + (int) (safeStart - startByte);
                    if (srcOffset < dataOffset) continue;
                    if (srcOffset + safeLen > dataOffset + dataLen) continue;

                    int destOffset = (int) (safeStart - 1);
                    if (destOffset < 0 || destOffset + safeLen > fileBuffer.length) continue;

                    int chunkOffset = (int) (safeStart - c.start);

                    // mark newly received bytes
                    for (int i = 0; i < safeLen; i++) {
                        int pos = chunkOffset + i;
                        if (!c.received[pos]) {
                            c.received[pos] = true;
                            newlyFilled++;
                        }
                    }

                    // write into final buffer
                    System.arraycopy(data, srcOffset, fileBuffer, destOffset, safeLen);

                    if (newlyFilled > 0) c.receivedCount += newlyFilled;

                    if (c.isComplete()) {
                        c.status = ChunkStatus.RECEIVED;
                        c.pathIndex = -1;
                        c.lastSentTimeNs = 0;
                        becameComplete = true;
                    } else {
                        c.status = ChunkStatus.NOT_REQUESTED;
                        c.pathIndex = -1;
                        c.lastSentTimeNs = 0;
                    }
                }

                synchronized (path) {
                    path.totalDataBytes += rangeLen;

                    if (newlyFilled > 0) {
                        path.bytesUniqueDelivered += newlyFilled;

                        // gentle additive increase on progress, bounded
                        if (path.windowSize < MAX_WINDOW) path.windowSize++;

                        if (sampleRttMs > 0) path.updateRtt(sampleRttMs);
                    } else {
                        // still useful RTT sample
                        if (sampleRttMs > 0) path.updateRtt(sampleRttMs);
                    }
                }

                if (newlyFilled > 0) uniqueBytesReceived.addAndGet(newlyFilled);

                if (becameComplete) {
                    int rem = remainingChunks.decrementAndGet();
                    if (rem <= 0) downloadDone.set(true);
                } else {
                    // still missing bytes -> request next hole
                    enqueueIfNeeded(c, pending);
                }
            }
        }
    }

    // ===== encoding =====

    private static byte[] encodeRequest(byte requestType, byte fileId, int startByte, int endByte) {
        if (requestType == REQ_GET_FILE_DATA) {
            if (startByte < 1) startByte = 1;
            if (endByte < startByte) endByte = startByte;
            int maxEnd = startByte + MAX_DATA_SIZE - 1;
            if (endByte > maxEnd) endByte = maxEnd;
        }

        ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        bb.put(requestType);
        bb.put(fileId);
        bb.putInt(startByte);
        bb.putInt(endByte);
        return bb.array();
    }

    // ===== md5 =====

    private static String computeMD5(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
