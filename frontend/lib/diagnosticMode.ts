export type DiagnosticMode = "SNAPSHOT" | "PARTIAL" | "FULL";

export type DiagnosticSection =
  | "EXECUTIVE"
  | "COMPETITIVE"
  | "SITE_EVAL"
  | "SITE_EVAL_SUMMARY"
  | "SITE_EVAL_FULL"
  | "REVENUE";

export function getDiagnosticMode(crawl_confidence?: string): DiagnosticMode {
  const c = String(crawl_confidence || "").trim().toLowerCase();
  if (c === "high") return "FULL";
  if (c === "medium") return "PARTIAL";
  return "SNAPSHOT";
}

export function shouldRenderSection(
  mode: DiagnosticMode,
  section: DiagnosticSection,
  unknownRatio = 0,
): boolean {
  const mostlyUnknown = unknownRatio > 0.5;
  if (mode === "SNAPSHOT") {
    return section === "EXECUTIVE" || section === "COMPETITIVE" || section === "SITE_EVAL";
  }
  if (mode === "PARTIAL") {
    if (section === "SITE_EVAL_FULL" || section === "REVENUE") return false;
    if (section === "SITE_EVAL_SUMMARY" && mostlyUnknown) return false;
    return section === "EXECUTIVE" || section === "COMPETITIVE" || section === "SITE_EVAL" || section === "SITE_EVAL_SUMMARY";
  }
  if (section === "SITE_EVAL_FULL" && mostlyUnknown) return false;
  return true;
}
