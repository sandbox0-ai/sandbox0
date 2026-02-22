import path from "node:path";

/**
 * @typedef {"fence" | "tabs"} ExampleSourceType
 * @typedef {"syntax" | "run"} ExampleMode
 * @typedef {"none" | "infra"} ExampleNeeds
 *
 * @typedef {Object} DocExample
 * @property {string} id
 * @property {string} sourceFile
 * @property {ExampleSourceType} sourceType
 * @property {string} language
 * @property {string} code
 * @property {ExampleMode} mode
 * @property {ExampleNeeds} needs
 * @property {string | undefined} runtime
 * @property {string[]} deps
 * @property {string[]} setup
 * @property {string[]} env
 * @property {string | undefined} session
 * @property {number} order
 * @property {Record<string, unknown>} metadata
 */

/**
 * @typedef {Object} ExtractIssue
 * @property {string} sourceFile
 * @property {string} reason
 */

/**
 * @typedef {Object} ManifestSummary
 * @property {number} total
 * @property {Record<string, number>} byLanguage
 * @property {Record<ExampleSourceType, number>} bySourceType
 */

/**
 * @typedef {Object} ExamplesManifest
 * @property {"1.0"} version
 * @property {string} generatedAt
 * @property {string} docsRoot
 * @property {ManifestSummary} summary
 * @property {DocExample[]} examples
 * @property {ExtractIssue[]} issues
 */

export const DEFAULT_MODE = "syntax";
export const DEFAULT_NEEDS = "none";

/**
 * A stable path representation for reproducible IDs.
 * @param {string} docsRoot
 * @param {string} filePath
 */
export function toRelativePosixPath(docsRoot, filePath) {
  return path.relative(docsRoot, filePath).split(path.sep).join("/");
}

/**
 * @param {DocExample[]} examples
 * @returns {ManifestSummary}
 */
export function buildSummary(examples) {
  const byLanguage = {};
  const bySourceType = { fence: 0, tabs: 0 };
  for (const item of examples) {
    byLanguage[item.language] = (byLanguage[item.language] || 0) + 1;
    bySourceType[item.sourceType] += 1;
  }

  return {
    total: examples.length,
    byLanguage,
    bySourceType,
  };
}

