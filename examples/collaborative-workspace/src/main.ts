import { Effect, Schema } from "effect";
import { MeridianClient } from "meridian-sdk";
import type { PresenceEntry } from "meridian-sdk";

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const TaskSchema = Schema.Struct({ text: Schema.String });
type Task = typeof TaskSchema.Type;

const TitleSchema = Schema.String;

const PresenceSchema = Schema.Struct({ name: Schema.String });
type PresenceData = typeof PresenceSchema.Type;

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

const $  = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const $title    = $<HTMLInputElement>("workspace-title");
const $taskForm = $<HTMLFormElement>("task-form");
const $taskInput = $<HTMLInputElement>("task-input");
const $taskList  = $<HTMLUListElement>("task-list");
const $voteCount = $("vote-count");
const $onlineList = $<HTMLUListElement>("online-list");
const $statusDot  = $("status-dot");
const $presenceBar = $("presence-bar");

// ---------------------------------------------------------------------------
// Connect overlay logic
// ---------------------------------------------------------------------------

$("connect-btn").addEventListener("click", async () => {
  const url   = $<HTMLInputElement>("url-input").value.trim();
  const ns    = $<HTMLInputElement>("ns-input").value.trim();
  const token = $<HTMLInputElement>("token-input").value.trim();
  const errEl = $("connect-error");

  if (!url || !ns || !token) {
    errEl.textContent = "All fields are required.";
    return;
  }

  errEl.textContent = "";
  $<HTMLButtonElement>("connect-btn").textContent = "Connecting…";

  const result = await Effect.runPromise(
    Effect.either(MeridianClient.create({ url, namespace: ns, token }))
  );

  if (result._tag === "Left") {
    errEl.textContent = String(result.left);
    $<HTMLButtonElement>("connect-btn").textContent = "Connect";
    return;
  }

  $("connect-overlay").style.display = "none";
  $("title-section").style.display = "flex";
  ($("main") as HTMLElement).style.display = "grid";

  initWorkspace(result.right, token);
});

// ---------------------------------------------------------------------------
// Workspace
// ---------------------------------------------------------------------------

function initWorkspace(client: MeridianClient, token: string): void {
  const myName = `Client #${client.clientId}`;

  // ---- Connection state ----
  // WsTransport exposes currentState via transport; use onStateChange indirectly
  // by polling — or just reflect once connected
  client.waitForConnected().then(() => {
    $statusDot.className = "connected";
  }).catch(() => {
    $statusDot.className = "";
  });

  // ---- LWWRegister — workspace title ----
  const titleReg = client.lwwregister<string>("title", TitleSchema);

  $title.addEventListener("input", () => {
    titleReg.set($title.value);
  });

  titleReg.onChange((v) => {
    if (v !== null && v !== $title.value) {
      $title.value = v;
    }
  });

  // Seed with current value if already set
  const initialTitle = titleReg.value();
  if (initialTitle !== null) $title.value = initialTitle;

  // ---- ORSet — task list ----
  const taskSet = client.orset<Task>("tasks", TaskSchema);

  function renderTasks(tasks: Task[]): void {
    $taskList.innerHTML = "";
    const sorted = [...tasks].sort((a, b) => a.text.localeCompare(b.text));
    for (const task of sorted) {
      const li = document.createElement("li");
      li.className = "task-item";
      li.innerHTML = `<span>${escapeHtml(task.text)}</span>`;
      const btn = document.createElement("button");
      btn.className = "task-remove";
      btn.textContent = "×";
      btn.addEventListener("click", () => taskSet.remove(task));
      li.appendChild(btn);
      $taskList.appendChild(li);
    }
  }

  taskSet.onChange(renderTasks);
  renderTasks(taskSet.elements());

  $taskForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const text = $taskInput.value.trim();
    if (!text) return;
    taskSet.add({ text });
    $taskInput.value = "";
  });

  // ---- PNCounter — votes ----
  const votes = client.pncounter("votes");

  votes.onChange((v) => {
    $voteCount.textContent = String(v);
  });
  $voteCount.textContent = String(votes.value());

  $("btn-up").addEventListener("click", () => votes.increment(1));
  $("btn-down").addEventListener("click", () => votes.decrement(1));

  // ---- Presence ----
  const presence = client.presence<PresenceData>("presence", PresenceSchema);

  function renderPresence(entries: PresenceEntry<PresenceData>[]): void {
    $onlineList.innerHTML = "";
    $presenceBar.innerHTML = "";

    for (const entry of entries) {
      const li = document.createElement("li");
      li.className = "presence-item";
      const dot = document.createElement("div");
      dot.className = "dot";
      const name = document.createElement("span");
      name.textContent = entry.data.name;
      li.appendChild(dot);
      li.appendChild(name);
      if (entry.clientId === client.clientId) {
        const you = document.createElement("span");
        you.className = "you";
        you.textContent = "you";
        li.appendChild(you);
      }
      $onlineList.appendChild(li);

      // Avatars in header
      const av = document.createElement("div");
      av.className = "avatar";
      av.textContent = entry.data.name[0]?.toUpperCase() ?? "?";
      av.title = entry.data.name;
      $presenceBar.appendChild(av);
    }
  }

  presence.onChange(renderPresence);

  // Send initial heartbeat once connected, then refresh every 15s (ttl = 30s)
  client.waitForConnected().then(() => {
    presence.heartbeat({ name: myName }, 30_000);
  }).catch(() => {});
  const heartbeatInterval = setInterval(() => {
    presence.heartbeat({ name: myName }, 30_000);
  }, 15_000);

  // Leave cleanly on tab close
  window.addEventListener("beforeunload", () => {
    clearInterval(heartbeatInterval);
    presence.leave();
    client.close();
  });
}

// ---------------------------------------------------------------------------
// Util
// ---------------------------------------------------------------------------

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
