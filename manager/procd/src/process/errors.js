export class ProcdError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

export const errors = {
  processNotFound: () => new ProcdError("process_not_found", "process not found"),
  processNotRunning: () => new ProcdError("process_not_running", "process not running"),
  processFinished: () => new ProcdError("process_finished", "process finished"),
  processAlreadyRunning: () => new ProcdError("process_already_running", "process already running"),
  unsupportedProcessType: () => new ProcdError("unsupported_process_type", "unsupported process type"),
  unsupportedLanguage: () => new ProcdError("unsupported_language", "unsupported language"),
  invalidCommand: (message = "invalid command") => new ProcdError("invalid_command", message),
  ptyUnavailable: () => new ProcdError("pty_unavailable", "pty not available"),
  signalFailed: (message = "signal failed") => new ProcdError("signal_failed", message),
  invalidPTYSize: () => new ProcdError("invalid_pty_size", "invalid pty size"),
  inputBufferFull: () => new ProcdError("input_buffer_full", "input buffer full")
};
