// 平台交互：共享 TriggerLink client（serve 端点 + 发事件 + 自注册）。
import { createClient } from "@triggerlink/sdk";

const PLATFORM_URL = process.env.TRIGGERLINK_PLATFORM_URL ?? "http://localhost:8288";
const EVENT_KEY = process.env.TRIGGERLINK_EVENT_KEY ?? "dev"; // 与平台 -event-key 一致

/** 共享 client：serve 端点、发事件（client.send）、自注册（client.register）共用。 */
export const triggerlink = createClient({
  id: "nextjs-demo",
  signingKey: process.env.TRIGGERLINK_SIGNING_KEY ?? "dev", // 与平台 -signing-key 一致
  eventKey: EVENT_KEY,
  baseUrl: PLATFORM_URL,
});
