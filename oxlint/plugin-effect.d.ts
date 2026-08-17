interface EffectOxlintRule {
  readonly meta: {
    readonly type: "problem";
    readonly docs: { readonly description: string };
    readonly messages: Readonly<Record<string, string>>;
  };
  readonly create: (context: {
    report(descriptor: { node: unknown; messageId: string }): void;
  }) => Readonly<Record<string, (node: unknown) => void>>;
}

declare const effectOxlintPlugin: {
  readonly meta: { readonly name: "dev-kit-effect" };
  readonly rules: Readonly<Record<string, EffectOxlintRule>>;
};

export default effectOxlintPlugin;
