import React, { useEffect, useId, useMemo, useRef, useState } from "react";

export type SelectProps = Omit<React.SelectHTMLAttributes<HTMLSelectElement>, "children"> & {
  children: React.ReactNode;
};

type SelectOption = {
  value: string;
  label: string;
  disabled?: boolean;
};

function optionsFromChildren(children: React.ReactNode): SelectOption[] {
  const options: SelectOption[] = [];
  React.Children.forEach(children, (child) => {
    if (!React.isValidElement(child) || child.type !== "option") {
      return;
    }
    const props = child.props as { value?: string | number; disabled?: boolean; children?: React.ReactNode };
    options.push({
      value: String(props.value ?? ""),
      label: String(props.children ?? ""),
      disabled: props.disabled,
    });
  });
  return options;
}

/** Compact themed dropdown; avoids oversized native selects in Windows WebView2. */
export function Select({
  children,
  className = "",
  value,
  defaultValue,
  onChange,
  disabled,
  id,
  "aria-label": ariaLabel,
  name,
}: SelectProps) {
  const listId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const options = useMemo(() => optionsFromChildren(children), [children]);
  const currentValue = value !== undefined ? String(value) : defaultValue !== undefined ? String(defaultValue) : options[0]?.value ?? "";
  const selected = options.find((option) => option.value === currentValue) ?? options[0];
  const classes = ["select-control", "select-custom__trigger", className].filter(Boolean).join(" ");

  useEffect(() => {
    if (!open) {
      return;
    }
    function onPointerDown(event: MouseEvent) {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  function pick(nextValue: string) {
    setOpen(false);
    onChange?.({
      target: { value: nextValue, name: name ?? "" },
      currentTarget: { value: nextValue, name: name ?? "" },
    } as React.ChangeEvent<HTMLSelectElement>);
  }

  return (
    <div className="select-custom" ref={rootRef}>
      <button
        type="button"
        id={id}
        name={name}
        className={classes}
        disabled={disabled}
        aria-label={ariaLabel}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        onClick={() => {
          if (!disabled) {
            setOpen((wasOpen) => !wasOpen);
          }
        }}
      >
        <span className="select-custom__value">{selected?.label ?? "—"}</span>
      </button>
      {open && !disabled && (
        <ul id={listId} className="select-custom__menu" role="listbox" aria-label={ariaLabel}>
          {options.map((option) => (
            <li key={option.value} role="presentation">
              <button
                type="button"
                role="option"
                className={`select-custom__option${option.value === currentValue ? " select-custom__option--active" : ""}`}
                aria-selected={option.value === currentValue}
                disabled={option.disabled}
                onClick={() => pick(option.value)}
              >
                {option.label}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
