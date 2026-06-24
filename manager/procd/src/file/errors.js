export class FileError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

export const fileErrors = {
  fileNotFound: () => new FileError("file_not_found", "file not found"),
  dirNotFound: () => new FileError("directory_not_found", "directory not found"),
  fileTooLarge: () => new FileError("file_too_large", "file too large"),
  permissionDenied: () => new FileError("permission_denied", "permission denied"),
  pathExists: () => new FileError("path_exists", "path already exists"),
  pathNotDir: () => new FileError("path_not_directory", "path is not a directory"),
  pathNotFile: () => new FileError("path_not_file", "path is not a file"),
  operationFailed: (message) => new FileError("operation_failed", message)
};
