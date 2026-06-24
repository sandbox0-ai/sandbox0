const levels = new Map([
  ["debug", 10],
  ["info", 20],
  ["warn", 30],
  ["error", 40]
]);

export class Logger {
  constructor(service, level = "info") {
    this.service = service;
    this.level = levels.get(level) || levels.get("info");
  }

  debug(message, fields = {}) {
    this.write("debug", message, fields);
  }

  info(message, fields = {}) {
    this.write("info", message, fields);
  }

  warn(message, fields = {}) {
    this.write("warn", message, fields);
  }

  error(message, fields = {}) {
    this.write("error", message, fields);
  }

  write(level, message, fields) {
    if ((levels.get(level) || 99) < this.level) return;
    const record = {
      level,
      ts: new Date().toISOString(),
      service: this.service,
      msg: message,
      ...fields
    };
    const line = JSON.stringify(record);
    if (level === "error" || level === "warn") {
      process.stderr.write(line + "\n");
    } else {
      process.stdout.write(line + "\n");
    }
  }
}
