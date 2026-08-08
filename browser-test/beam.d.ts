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
     * Creates a new BEAM node with in-memory storage.
     *
     * Data is lost when the page reloads. For persistence, use
     * [`new_persistent()`](Self::new_persistent) instead.
     *
     * Call `connect()` to join a relay mesh, then `put()` / `get()` to read/write.
     */
    constructor();
    /**
     * Creates a new BEAM node with IndexedDB persistent storage.
     *
     * Data survives page reloads. The IndexedDB database opens
     * asynchronously — writes are buffered until the DB is ready,
     * then flushed automatically.
     *
     * ```js
     * const beam = Beam.new_persistent();
     * beam.connect("wss://relay.example.com/ws");
     * beam.put("app.theme", "dark");
     * // Reload page — "app.theme" is still "dark"
     * ```
     */
    static new_persistent(): Beam;
    /**
     * Writes a value to the graph at the given path.
     *
     * The value is replicated to all connected peers. If no peers are
     * connected yet, the value is stored locally and will sync when
     * a connection is established.
     *
     * # Arguments
     *
     * * `path` - Dot-separated path (e.g. `"users.alice.name"`)
     * * `value` - Value to store (string, number, boolean, or null)
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

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_beam_free: (a: number, b: number) => void;
    readonly beam_connect: (a: number, b: number, c: number) => void;
    readonly beam_new: () => number;
    readonly beam_new_persistent: () => number;
    readonly beam_put: (a: number, b: number, c: number, d: number, e: number) => void;
    readonly beam_put_bool: (a: number, b: number, c: number, d: number) => void;
    readonly beam_put_null: (a: number, b: number, c: number) => void;
    readonly beam_put_num: (a: number, b: number, c: number, d: number) => void;
    readonly beam_stop: (a: number) => void;
    readonly __wasm_bindgen_func_elem_209: (a: number, b: number, c: number) => void;
    readonly __wasm_bindgen_func_elem_209_1: (a: number, b: number, c: number) => void;
    readonly __wasm_bindgen_func_elem_209_2: (a: number, b: number, c: number) => void;
    readonly __wbindgen_export: (a: number, b: number) => number;
    readonly __wbindgen_export2: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_export3: (a: number) => void;
    readonly __wbindgen_export4: (a: number, b: number, c: number) => void;
    readonly __wbindgen_export5: (a: number, b: number) => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
