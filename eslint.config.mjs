import { defineConfig } from "eslint/config";
import globals from "globals";
import js from "@eslint/js";
import stylistic from "@stylistic/eslint-plugin";

// ESLint configuration
//
// Customize this file in consultation with the rest of your team to ensure that
// your team's JavaScript style is followed by everyone. See the ESLint
// documentation for more information: https://eslint.org/docs/latest/use/configure/

const commonRules = {
  // Core best practices
  "camelcase": ["error"],
  "no-use-before-define": ["error", {
    allowNamedExports: true,
  }],
  "no-var": ["error"],

  // Stylistic rules
  "stylistic/comma-dangle": ["error", "always-multiline"],
  "stylistic/comma-spacing": ["error", {
    before: false,
    after: true,
  }],
  "stylistic/indent": ["error", 2],
  "stylistic/no-trailing-spaces": ["error"],
  "stylistic/object-curly-spacing": ["error", "always"],
  "stylistic/semi": ["error", "always", {
    omitLastInOneLineBlock: true,
  }],
};

export default defineConfig([
  // Rules for Node.js files located in the root directory and tasks directory
  {
    extends: ['js/recommended'],
    files: ["*.mjs", "tasks/**/*.mjs", "tasks/**/*.js"],
    plugins: { js, stylistic },
    rules: commonRules,
    languageOptions: {
      globals: {
        ...globals.node,
      },
      ecmaVersion: "latest",
      sourceType: "module",
    },
  },
  // Rules for browser-based JavaScript files located in the ui directory
  {
    extends: ['js/recommended'],
    files: ["ui/**/*.js"],
    plugins: { js, stylistic },
    rules: commonRules,
    languageOptions: {
      globals: {
        ...globals.browser,
      },
      ecmaVersion: "latest",
      sourceType: "module",
    },
  },
]);