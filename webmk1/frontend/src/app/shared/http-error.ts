export function httpErrorMessage(error: unknown, fallback: string): string {
  const detail = (error as { error?: { detail?: unknown } })?.error?.detail;

  if (typeof detail === 'string') {
    return detail;
  }

  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        if (typeof item === 'string') {
          return item;
        }
        if (item && typeof item === 'object' && 'msg' in item) {
          return String((item as { msg: unknown }).msg);
        }
        return JSON.stringify(item);
      })
      .join(' | ');
  }

  if (detail && typeof detail === 'object') {
    if ('msg' in detail) {
      return String((detail as { msg: unknown }).msg);
    }
    return JSON.stringify(detail);
  }

  return fallback;
}
