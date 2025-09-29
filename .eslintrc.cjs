module.exports = {
  root: true,
  env: {
    browser: true,
    node: true,
    es2020: true,
  },
  parser: "vue-eslint-parser",
  parserOptions: {
    parser: "@babel/eslint-parser",
    requireConfigFile: false,
    ecmaVersion: 2020,
    sourceType: "module"
  },
  extends: [
    "plugin:vue/essential",
    "eslint:recommended",
    "plugin:prettier/recommended"
  ],
  plugins: ["vue"],
  rules: {
    "semi": ["error", "always"],
    "quotes": ["error", "single", { "avoidEscape": true }],
    "no-unused-vars": ["warn"],
    "no-console": ["off"],
    "vue/no-unused-components": "warn"
  }
};
