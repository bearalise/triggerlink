// 平台↔应用 HMAC-SHA256 签名验证（协议第 2 节）。
// 头部格式：t=<unix秒>,v1=<hex(hmac_sha256(key, "<t>.<body>"))>
import { createHmac, timingSafeEqual } from "node:crypto";

const TOLERANCE_SEC = 5 * 60; // ±5 分钟，与 Go SDK 一致

function computeSig(key: string, ts: string, body: Uint8Array): string {
  const mac = createHmac("sha256", key);
  mac.update(`${ts}.`);
  mac.update(body);
  return mac.digest("hex");
}

/** 生成签名头部值（平台侧模拟、测试用；SDK 正常路径只验签）。 */
export function sign(key: string, body: Uint8Array, now: Date = new Date()): string {
  const t = Math.floor(now.getTime() / 1000).toString();
  return `t=${t},v1=${computeSig(key, t, body)}`;
}

/** 校验签名头部值；不合法一律返回 false（调用方应回 401）。 */
export function verifySignature(key: string, header: string | null, body: Uint8Array): boolean {
  if (!header) return false;
  let ts = "";
  let sig = "";
  for (const part of header.split(",")) {
    if (part.startsWith("t=")) ts = part.slice(2);
    else if (part.startsWith("v1=")) sig = part.slice(3);
  }
  if (!ts || !sig) return false;
  const sec = Number(ts);
  if (!Number.isFinite(sec)) return false;
  const skew = Math.abs(Date.now() / 1000 - sec);
  if (skew > TOLERANCE_SEC) return false;
  const expected = computeSig(key, ts, body);
  const a = Buffer.from(expected, "utf8");
  const b = Buffer.from(sig, "utf8");
  return a.length === b.length && timingSafeEqual(a, b);
}
