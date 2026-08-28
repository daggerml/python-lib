export function StatusPill({ value = "unknown" }: { value?: string }) {
  const normalized = value.toLowerCase().replaceAll("_", "-");
  return (
    <span className={`status status--${normalized}`} aria-label={`Status: ${value}`}>
      <span className="status__dot" aria-hidden="true" />
      {value.replaceAll("_", " ")}
    </span>
  );
}
