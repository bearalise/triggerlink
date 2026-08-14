import type { Metadata } from "next";

export const metadata: Metadata = { title: "TriggerLink Next.js 示例" };

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}
