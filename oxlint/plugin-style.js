import stylisticPlugin from "@stylistic/eslint-plugin";

export default {
  meta: { name: "dev-kit-style" },
  rules: {
    "padding-line-between-statements": stylisticPlugin.rules["padding-line-between-statements"],
  },
};
