from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColorTokens:
    background: str = "#0f172a"
    surface: str = "#111827"
    surface_muted: str = "#1f2937"
    border: str = "#334155"
    text: str = "#e5e7eb"
    text_muted: str = "#94a3b8"
    primary: str = "#60a5fa"
    primary_hover: str = "#3b82f6"
    primary_text: str = "#ffffff"
    success: str = "#86efac"
    success_surface: str = "#14532d"
    warning: str = "#facc15"
    warning_surface: str = "#713f12"
    error: str = "#fca5a5"
    error_surface: str = "#7f1d1d"


@dataclass(frozen=True)
class SpacingTokens:
    xs: int = 4
    sm: int = 8
    md: int = 12
    lg: int = 16
    xl: int = 24


@dataclass(frozen=True)
class TypographyTokens:
    body: int = 13
    small: int = 12
    heading: int = 18
    section: int = 15


@dataclass(frozen=True)
class RadiusTokens:
    sm: int = 4
    md: int = 6
    lg: int = 10


@dataclass(frozen=True)
class DesignTokens:
    colors: ColorTokens = field(default_factory=ColorTokens)
    spacing: SpacingTokens = field(default_factory=SpacingTokens)
    typography: TypographyTokens = field(default_factory=TypographyTokens)
    radius: RadiusTokens = field(default_factory=RadiusTokens)


DEFAULT_TOKENS = DesignTokens()
