import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import { fileErrors } from "./errors.js";

function watchID() {
  return "watch-" + crypto.randomBytes(4).toString("hex");
}

function eventType(filename) {
  return filename ? "write" : "invalidate";
}

export class Watcher extends EventEmitter {
  constructor(root, recursive) {
    super();
    this.id = watchID();
    this.root = root;
    this.recursive = recursive;
    this.watchers = new Map();
    this.closed = false;
  }

  start() {
    if (!fs.existsSync(this.root)) fs.mkdirSync(this.root, { recursive: true, mode: 0o755 });
    if (!fs.statSync(this.root).isDirectory()) throw fileErrors.pathNotDir();
    this.addDir(this.root);
    if (this.recursive) {
      for (const dir of walkDirs(this.root)) this.addDir(dir);
    }
  }

  addDir(dir) {
    if (this.closed || this.watchers.has(dir)) return;
    let watcher;
    try {
      watcher = fs.watch(dir, { persistent: false }, (_event, filename) => {
        const eventPath = filename ? path.join(dir, filename.toString()) : dir;
        if (this.recursive) {
          try {
            if (fs.existsSync(eventPath) && fs.statSync(eventPath).isDirectory()) this.addDir(eventPath);
          } catch {
            // Best effort.
          }
        }
        this.emit("event", {
          watch_id: this.id,
          type: eventType(filename),
          path: eventPath
        });
      });
    } catch (err) {
      throw err;
    }
    this.watchers.set(dir, watcher);
  }

  close() {
    this.closed = true;
    for (const watcher of this.watchers.values()) watcher.close();
    this.watchers.clear();
  }
}

function* walkDirs(root) {
  const entries = fs.readdirSync(root, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const full = path.join(root, entry.name);
    yield full;
    yield* walkDirs(full);
  }
}

export class WatcherManager {
  constructor() {
    this.watchers = new Map();
  }

  watchDir(pathValue, recursive) {
    const watcher = new Watcher(pathValue, recursive);
    watcher.start();
    this.watchers.set(watcher.id, watcher);
    return watcher;
  }

  unwatchDir(id) {
    const watcher = this.watchers.get(id);
    if (!watcher) return;
    watcher.close();
    this.watchers.delete(id);
  }

  emit(event) {
    for (const watcher of this.watchers.values()) {
      if (event.path === watcher.root || (watcher.recursive && String(event.path).startsWith(watcher.root + path.sep))) {
        watcher.emit("event", { ...event, watch_id: watcher.id });
      }
    }
  }

  close() {
    for (const id of [...this.watchers.keys()]) this.unwatchDir(id);
  }
}
