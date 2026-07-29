import { MesErrorCode } from "./types.js";

export function invalidArgument(message: string): RangeError {
  const error = new RangeError(message) as RangeError & { code: number };
  error.code = MesErrorCode.InvalidArg;
  return error;
}

export function validatePort(port: number | undefined): void {
  if (port !== undefined && (!Number.isInteger(port) || port < 1 || port > 65535)) {
    throw invalidArgument(`port must be 1-65535, got ${port}`);
  }
}
