import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  transpilePackages: ["@sandbox0/ui", "@sandbox0/dashboard-core"],
  // Prevent webpack from trying to bundle Node.js-only dependencies from the
  // sandbox0 SDK (e.g. node:crypto used in webhook_signature.js). These are
  // only accessed server-side and must be resolved by the Node.js runtime.
  serverExternalPackages: ["sandbox0"],
  webpack(config, { isServer }) {
    if (isServer) {
      // Webpack 5 does not handle the "node:" URI scheme for built-in modules.
      // Mark them as CommonJS externals so Node.js resolves them at runtime
      // instead of webpack trying to read them as file-system paths.
      const existingExternals = Array.isArray(config.externals)
        ? config.externals
        : config.externals
          ? [config.externals]
          : [];
      config.externals = [
        ...existingExternals,
        (
          { request }: { request?: string },
          callback: (err?: Error | null, result?: string) => void,
        ) => {
          if (request?.startsWith("node:")) {
            return callback(null, `commonjs ${request}`);
          }
          callback();
        },
      ];
    }
    return config;
  },
};

export default nextConfig;
