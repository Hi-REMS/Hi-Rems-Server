module.exports = {
  root: true,
  env: { node: true, es2021: true },
  parserOptions: { ecmaVersion: 2021, sourceType: 'commonjs' },
  extends: ['eslint:recommended'],
  rules: {
    semi: ['error', 'always'],
    quotes: ['error', 'single', { avoidEscape: true }],
    'no-unused-vars': 'warn',
    'no-console': 'off',
  },
  ignorePatterns: ['frontend/**', 'node_modules/', 'dist/', 'coverage/'],
};
