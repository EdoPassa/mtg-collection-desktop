import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { ProbabilityGauge } from "./ProbabilityGauge";

afterEach(() => cleanup());

describe("ProbabilityGauge", () => {
  it("renders the formatted value and an accessible label", () => {
    render(<ProbabilityGauge value={0.42} formatted="42.0%" label="Draw ≥1" />);
    const gauge = screen.getByRole("img", { name: "Draw ≥1: 42.0%" });
    expect(gauge).toBeInTheDocument();
    expect(gauge).toHaveTextContent("42.0%");
  });

  it("falls back to a dash when the value is missing", () => {
    render(<ProbabilityGauge value={undefined} label="Land" />);
    expect(screen.getByRole("img", { name: "Land: —" })).toBeInTheDocument();
  });

  it("computes a percentage when no formatted string is provided", () => {
    render(<ProbabilityGauge value={0.5} />);
    expect(screen.getByRole("img", { name: "50.0%" })).toBeInTheDocument();
  });
});
