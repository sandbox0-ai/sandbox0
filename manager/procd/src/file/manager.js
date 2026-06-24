import fs from "node:fs";
import path from "node:path";
import { WatcherManager } from "./watcher.js";
import { fileErrors, FileError } from "./errors.js";

export const maxFileSize = 100 * 1024 * 1024;

function mapFSError(err, notFoundFactory = fileErrors.fileNotFound) {
  if (err instanceof FileError) return err;
  switch (err?.code) {
    case "ENOENT":
      return notFoundFactory();
    case "EACCES":
    case "EPERM":
      return fileErrors.permissionDenied();
    case "EISDIR":
      return fileErrors.pathNotFile();
    case "ENOTDIR":
      return fileErrors.pathNotDir();
    case "EEXIST":
      return fileErrors.pathExists();
    default:
      return fileErrors.operationFailed(err?.message || String(err));
  }
}

function fileInfoFromStats(requestPath, fullPath, stats) {
  const info = {
    name: path.basename(fullPath),
    path: requestPath,
    type: stats.isDirectory() ? "dir" : "file",
    size: stats.size,
    mode: (stats.mode & 0o777).toString(8).padStart(4, "0"),
    mod_time: stats.mtime.toISOString(),
    is_link: stats.isSymbolicLink()
  };
  if (stats.isSymbolicLink()) {
    info.type = "symlink";
    try {
      info.link_target = fs.readlinkSync(fullPath);
    } catch {
      // Best effort.
    }
  }
  return info;
}

export class FileManager {
  constructor(rootPath) {
    this.rootPath = rootPath || "/workspace";
    try {
      fs.mkdirSync(this.rootPath, { recursive: true, mode: 0o755 });
    } catch {
      // File operations will surface permission or missing-path errors later.
    }
    this.watchers = new WatcherManager();
  }

  sanitize(input) {
    const clean = path.resolve(path.isAbsolute(input) ? input : path.join(this.rootPath, input || "."));
    return clean;
  }

  readFile(requestPath) {
    try {
      return fs.readFileSync(this.sanitize(requestPath));
    } catch (err) {
      throw mapFSError(err);
    }
  }

  writeFile(requestPath, data, mode = 0o644) {
    if (data.length > maxFileSize) throw fileErrors.fileTooLarge();
    const full = this.sanitize(requestPath);
    try {
      if (fs.existsSync(full) && fs.statSync(full).isDirectory()) throw fileErrors.pathNotFile();
      fs.mkdirSync(path.dirname(full), { recursive: true, mode: 0o755 });
      const tmp = full + ".tmp";
      fs.writeFileSync(tmp, data, { mode });
      fs.renameSync(tmp, full);
      this.watchers.emit({ type: "write", path: full });
    } catch (err) {
      throw mapFSError(err);
    }
  }

  stat(requestPath) {
    const full = this.sanitize(requestPath);
    try {
      return fileInfoFromStats(requestPath, full, fs.lstatSync(full));
    } catch (err) {
      throw mapFSError(err);
    }
  }

  listDir(requestPath) {
    const full = this.sanitize(requestPath);
    try {
      const entries = fs.readdirSync(full, { withFileTypes: true });
      return entries.map((entry) => {
        const entryPath = path.join(full, entry.name);
        return fileInfoFromStats(path.join(requestPath, entry.name), entryPath, fs.lstatSync(entryPath));
      });
    } catch (err) {
      throw mapFSError(err, fileErrors.dirNotFound);
    }
  }

  makeDir(requestPath, recursive) {
    const full = this.sanitize(requestPath);
    try {
      if (recursive) fs.mkdirSync(full, { recursive: true, mode: 0o755 });
      else fs.mkdirSync(full, { mode: 0o755 });
      this.watchers.emit({ type: "create", path: full });
    } catch (err) {
      throw mapFSError(err);
    }
  }

  move(source, destination) {
    const src = this.sanitize(source);
    const dst = this.sanitize(destination);
    try {
      fs.mkdirSync(path.dirname(dst), { recursive: true, mode: 0o755 });
      fs.renameSync(src, dst);
      this.watchers.emit({ type: "rename", path: dst, old_path: src });
    } catch (err) {
      throw mapFSError(err);
    }
  }

  remove(requestPath) {
    const full = this.sanitize(requestPath);
    try {
      fs.rmSync(full, { recursive: true, force: true });
      this.watchers.emit({ type: "remove", path: full });
    } catch (err) {
      throw mapFSError(err);
    }
  }

  subscribeWatch(requestPath, recursive, handler) {
    const full = this.sanitize(requestPath);
    const watcher = this.watchers.watchDir(full, recursive);
    const listener = (event) => handler(event);
    watcher.on("event", listener);
    return {
      watcher,
      unsubscribe: () => {
        watcher.off("event", listener);
        this.watchers.unwatchDir(watcher.id);
      }
    };
  }

  close() {
    this.watchers.close();
  }
}
