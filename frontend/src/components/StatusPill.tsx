export function StatusPill({ status }: { status: string }) {
  const variant = status.startsWith("error") ? "error" : status === "loading" ? "loading" : "ready";

  return (
    <span className={`status-pill status-pill--${variant}`} title={status}>
      Resolver: {status}
    </span>
  );
}
