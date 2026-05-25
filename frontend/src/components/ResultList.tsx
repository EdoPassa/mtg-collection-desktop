export function ResultList({ title, rows, warn }: { title: string; rows: string[]; warn?: boolean }) {
  return (
    <div className={`result-list${warn ? " result-list--warn" : ""}`}>
      <h3>{title}</h3>
      {rows.length === 0 ? (
        <p>None.</p>
      ) : (
        <ul>
          {rows.map((row, index) => (
            <li key={`${row}-${index}`}>{row}</li>
          ))}
        </ul>
      )}
    </div>
  );
}
