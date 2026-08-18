/* tslint:disable */
/* eslint-disable */

/**
 * JavaScript-facing BEAM API.
 *
 * Wraps a [`Node`] and exposes a simplified interface for browser use.
 * Each `Beam` instance is an independent node in the P2P mesh.
 */
export class Beam {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Connects to a relay server via WebSocket.
     *
     * The connection is asynchronous — data will start flowing once the
     * WebSocket handshake completes. You can call `put()` immediately;
     * messages will be queued and sent once connected.
     *
     * # Arguments
     *
     * * `url` - WebSocket URL (e.g. `"wss://relay.example.com/ws"`)
     */
    connect(url: string): void;
    /**
     * Reads the value at the given path once.
     *
     * Returns a `Promise` that resolves to the value (string) or `null`
     * if not found within the timeout (default 66ms, matching Gun.js).
     *
     * ```js
     * const val = await beam.get("chat.123");
     * if (val) console.log("got:", val);
     * ```
     */
    get(path: string): Promise<any>;
    /**
     * Creates a new BEAM node with in-memory storage.
     *
     * Data is lost when the page reloads. For persistence, use
     * [`new_persistent()`](Self::new_persistent) instead.
     */
    constructor();
    /**
     * Creates a new BEAM node with IndexedDB persistent storage.
     *
     * Data survives page reloads. The IndexedDB database opens
     * asynchronously — writes are buffered until the DB is ready,
     * then flushed automatically.
     */
    static new_persistent(): Beam;
    /**
     * Subscribes to child updates at the given path.
     *
     * Uses Gun.js `.on()` semantics: the callback fires for each child
     * value under the path, not just the path's own value. For example,
     * `beam.on("chat", cb)` fires for each message written to
     * `chat.<timestamp>`.
     *
     * The subscription lives until `stop()` is called or the `Beam`
     * instance is dropped.
     *
     * ```js
     * beam.on("chat", (value) => {
     *   console.log("new message:", value);
     * });
     * ```
     */
    on(path: string, callback: Function): void;
    /**
     * Writes a string value to the graph at the given path.
     *
     * # Arguments
     *
     * * `path` - Dot-separated path (e.g. `"chat.123"`)
     * * `value` - String to store
     */
    put(path: string, value: string): void;
    /**
     * Writes a boolean value to the graph at the given path.
     */
    put_bool(path: string, value: boolean): void;
    /**
     * Writes a null value to the graph at the given path.
     */
    put_null(path: string): void;
    /**
     * Writes a numeric value to the graph at the given path.
     */
    put_num(path: string, value: number): void;
    /**
     * Stops the node and closes all connections.
     */
    stop(): void;
}

/**
 * Entry point invoked by JavaScript in a worker.
 */
export function task_worker_entry_point(ptr: number): void;
