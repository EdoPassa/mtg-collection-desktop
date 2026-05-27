type ProgressBarProps = {
  current: number;
  total: number;
  label?: string;
  visible: boolean;
};

const MAX_LABEL_LENGTH = 72;

function truncateLabel(label: string): string {
  const trimmed = label.trim();
  if (trimmed.length <= MAX_LABEL_LENGTH) {
    return trimmed;
  }
  return `${trimmed.slice(0, MAX_LABEL_LENGTH - 1)}…`;
}

export function ProgressBar({ current, total, label, visible }: ProgressBarProps) {
  if (!visible || total <= 0) {
    return null;
  }

  const safeCurrent = Math.min(Math.max(current, 0), total);
  const detail = label?.trim() ? truncateLabel(label) : null;

  return (
    <div className="import-progress" role="group" aria-label="Import validation progress">
      <div className="import-progress__header">
        <span className="import-progress__status">
          Resolving cards… {safeCurrent} / {total}
        </span>
        {detail && <span className="import-progress__label card-meta">{detail}</span>}
      </div>
      <progress
        className="import-progress__bar"
        value={safeCurrent}
        max={total}
        aria-valuenow={safeCurrent}
        aria-valuemin={0}
        aria-valuemax={total}
      />
    </div>
  );
}
