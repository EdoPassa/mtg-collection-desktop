import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { ProgressBar } from "./ProgressBar";

afterEach(() => cleanup());

describe("ProgressBar", () => {
  it("renders determinate progress and status text when visible", () => {
    render(<ProgressBar current={2} total={5} label="Lightning Bolt" visible />);

    const bar = screen.getByRole("progressbar");
    expect(bar).toHaveAttribute("aria-valuenow", "2");
    expect(bar).toHaveAttribute("aria-valuemax", "5");
    expect(screen.getByText("Resolving cards… 2 / 5")).toBeInTheDocument();
    expect(screen.getByText("Lightning Bolt")).toBeInTheDocument();
  });

  it("hides when not visible or total is zero", () => {
    const { rerender } = render(<ProgressBar current={1} total={5} visible={false} />);
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();

    rerender(<ProgressBar current={0} total={0} visible />);
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  });
});
