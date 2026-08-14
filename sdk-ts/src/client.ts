// Client：应用级配置（协议第 2/3 节）+ 事件发送与自注册（协议第 9 节）。
export interface ClientOptions {
  /** 应用标识，如 "web"，出现在内省清单中 */
  id: string;
  /** 与平台 -signing-key 共享的签名密钥 */
  signingKey: string;
  /** 与平台 -event-key 一致；send / register 需要 */
  eventKey?: string;
  /** 平台地址；send / register 需要，缺省 "http://localhost:8288" */
  baseUrl?: string;
}

/** send 的入参事件（协议第 9 节）。id 缺省平台生成；提供则按 ID 幂等去重。 */
export interface SendEventInput<T = unknown> {
  id?: string;
  name: string;
  data?: T;
  ts?: string; // RFC3339，缺省平台生成
}

export interface SendResult {
  ids: string[];
  status: number;
}

export interface RegisterOptions {
  /** 重试间隔毫秒，缺省 2000 */
  intervalMs?: number;
  /** 最大尝试次数，缺省 150（≈5 分钟，覆盖平台晚于应用启动的场景） */
  attempts?: number;
}

export interface Client {
  readonly id: string;
  readonly signingKey: string;
  /** 发送单个事件或事件数组到平台（仿 Inngest `inngest.send`）。 */
  send<T = unknown>(events: SendEventInput<T> | SendEventInput<T>[]): Promise<SendResult>;
  /**
   * 向平台管理 API 自注册本应用（POST /api/v1/apps），等价于平台 -app 静态内省的反向操作。
   * 平台会为内省回调本应用的 serve URL，因此需要在应用可对外服务之后调用；
   * 后台异步重试，不阻塞启动。等价于重复调用可兼作函数变更后的 sync。
   */
  register(serveUrl: string, opts?: RegisterOptions): void;
}

export function createClient(opts: ClientOptions): Client {
  if (!opts.id) throw new Error("createClient: id is required");
  if (!opts.signingKey) throw new Error("createClient: signingKey is required");
  const baseUrl = (opts.baseUrl ?? "http://localhost:8288").replace(/\/+$/, "");

  async function send(events: SendEventInput | SendEventInput[]): Promise<SendResult> {
    if (!opts.eventKey) throw new Error("client.send: eventKey is required (createClient 时传入)");
    const list = Array.isArray(events) ? events : [events];
    if (list.length === 0) throw new Error("client.send: events must not be empty");
    for (const e of list) {
      if (!e.name) throw new Error("client.send: event.name is required");
    }
    const resp = await fetch(`${baseUrl}/v1/events`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${opts.eventKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(Array.isArray(events) ? list : list[0]),
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`client.send: platform returned ${resp.status}: ${text}`);
    }
    return (await resp.json()) as SendResult;
  }

  function register(serveUrl: string, regOpts: RegisterOptions = {}): void {
    if (!opts.eventKey) throw new Error("client.register: eventKey is required (createClient 时传入)");
    if (!serveUrl) throw new Error("client.register: serveUrl is required");
    const intervalMs = regOpts.intervalMs ?? 2000;
    const maxAttempts = regOpts.attempts ?? 150;
    let attempts = 0;
    const tryRegister = async () => {
      attempts++;
      try {
        const resp = await fetch(`${baseUrl}/api/v1/apps`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${opts.eventKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ url: serveUrl }),
        });
        if (resp.ok) {
          console.log(`[triggerlink] registered ${serveUrl} (platform ${resp.status})`);
          return;
        }
        console.warn(`[triggerlink] register #${attempts}: platform HTTP ${resp.status}`);
      } catch (err) {
        console.warn(
          `[triggerlink] register #${attempts}: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
      if (attempts < maxAttempts) setTimeout(tryRegister, intervalMs);
      else console.warn("[triggerlink] register gave up, please POST /api/v1/apps manually");
    };
    void tryRegister();
  }

  return { id: opts.id, signingKey: opts.signingKey, send, register };
}
