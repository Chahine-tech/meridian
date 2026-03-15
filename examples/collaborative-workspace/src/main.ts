import { Effect, Schema } from "effect";
import { MeridianClient } from "meridian-sdk";
import type { PresenceEntry, CrdtMapValue } from "meridian-sdk";

const TaskSchema = Schema.Struct({ text: Schema.String });
type Task = typeof TaskSchema.Type;

const TitleSchema = Schema.String;

const PresenceSchema = Schema.Struct({ name: Schema.String });
type PresenceData = typeof PresenceSchema.Type;

const getElement = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;

const workspaceTitleInput = getElement<HTMLInputElement>("workspace-title");
const taskForm = getElement<HTMLFormElement>("task-form");
const taskInput = getElement<HTMLInputElement>("task-input");
const taskList = getElement<HTMLUListElement>("task-list");
const voteCount = getElement("vote-count");
const onlineList = getElement<HTMLUListElement>("online-list");
const statusDot = getElement("status-dot");
const presenceBar = getElement("presence-bar");
const docAuthorEl = getElement("doc-author");
const docVersionEl = getElement("doc-version");
const docStatusEl = getElement("doc-status");
const docStatusSelect = getElement<HTMLSelectElement>("doc-status-select");
const docVersionBtn = getElement("doc-version-btn");

getElement("connect-btn").addEventListener("click", async () => {
  const url = getElement<HTMLInputElement>("url-input").value.trim();
  const ns = getElement<HTMLInputElement>("ns-input").value.trim();
  const token = getElement<HTMLInputElement>("token-input").value.trim();
  const errorEl = getElement("connect-error");

  if (!url || !ns || !token) {
    errorEl.textContent = "All fields are required.";
    return;
  }

  errorEl.textContent = "";
  getElement<HTMLButtonElement>("connect-btn").textContent = "Connecting…";

  const result = await Effect.runPromise(
    Effect.either(MeridianClient.create({ url, namespace: ns, token }))
  );

  if (result._tag === "Left") {
    errorEl.textContent = String(result.left);
    getElement<HTMLButtonElement>("connect-btn").textContent = "Connect";
    return;
  }

  getElement("connect-overlay").style.display = "none";
  getElement("title-section").style.display = "flex";
  (getElement("main") as HTMLElement).style.display = "grid";

  initWorkspace(result.right);
});

const escapeHtml = (str: string): string =>
  str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

const renderPresence = (entries: PresenceEntry<PresenceData>[], clientId: number): void => {
  onlineList.innerHTML = "";
  presenceBar.innerHTML = "";

  for (const entry of entries) {
    const listItem = document.createElement("li");
    listItem.className = "presence-item";
    const dot = document.createElement("div");
    dot.className = "dot";
    const nameEl = document.createElement("span");
    nameEl.textContent = entry.data.name;
    listItem.appendChild(dot);
    listItem.appendChild(nameEl);
    if (entry.clientId === clientId) {
      const youLabel = document.createElement("span");
      youLabel.className = "you";
      youLabel.textContent = "you";
      listItem.appendChild(youLabel);
    }
    onlineList.appendChild(listItem);

    const avatar = document.createElement("div");
    avatar.className = "avatar";
    avatar.textContent = entry.data.name[0]?.toUpperCase() ?? "?";
    avatar.title = entry.data.name;
    presenceBar.appendChild(avatar);
  }
};

const initWorkspace = (client: MeridianClient): void => {
  const myName = `Client #${client.clientId}`;

  client.waitForConnected().then(() => {
    statusDot.className = "connected";
  }).catch(() => {
    statusDot.className = "";
  });

  const titleReg = client.lwwregister<string>("title", TitleSchema);

  workspaceTitleInput.addEventListener("input", () => {
    titleReg.set(workspaceTitleInput.value);
  });

  titleReg.onChange((value: string | null) => {
    if (value !== null && value !== workspaceTitleInput.value) {
      workspaceTitleInput.value = value;
    }
  });

  const initialTitle = titleReg.value();
  if (initialTitle !== null) workspaceTitleInput.value = initialTitle;

  const taskSet = client.orset<Task>("tasks", TaskSchema);

  const renderTasks = (tasks: Task[]): void => {
    taskList.innerHTML = "";
    const sorted = [...tasks].sort((a, b) => a.text.localeCompare(b.text));
    for (const task of sorted) {
      const listItem = document.createElement("li");
      listItem.className = "task-item";
      listItem.innerHTML = `<span>${escapeHtml(task.text)}</span>`;
      const removeBtn = document.createElement("button");
      removeBtn.className = "task-remove";
      removeBtn.textContent = "×";
      removeBtn.addEventListener("click", () => taskSet.remove(task));
      listItem.appendChild(removeBtn);
      taskList.appendChild(listItem);
    }
  };

  taskSet.onChange(renderTasks);
  renderTasks(taskSet.elements());

  taskForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const text = taskInput.value.trim();
    if (!text) return;
    taskSet.add({ text });
    taskInput.value = "";
  });

  const votes = client.pncounter("votes");

  votes.onChange((value: number) => {
    voteCount.textContent = String(value);
  });
  voteCount.textContent = String(votes.value());

  getElement("btn-up").addEventListener("click", () => votes.increment(1));
  getElement("btn-down").addEventListener("click", () => votes.decrement(1));

  const presence = client.presence<PresenceData>("presence", PresenceSchema);

  presence.onChange((entries: PresenceEntry<PresenceData>[]) => renderPresence(entries, client.clientId));

  client.waitForConnected().then(() => {
    presence.heartbeat({ name: myName }, 30_000);
  }).catch(() => {});

  const heartbeatInterval = setInterval(() => {
    presence.heartbeat({ name: myName }, 30_000);
  }, 15_000);

  window.addEventListener("beforeunload", () => {
    clearInterval(heartbeatInterval);
    presence.leave();
    client.close();
  });

  // CRDTMap — doc metadata: author (LwwRegister), version (GCounter), status (LwwRegister)
  const docMeta = client.crdtmap("doc-meta");

  const renderDocMeta = (value: CrdtMapValue): void => {
    const author = (value["author"] as { value?: string } | undefined)?.value;
    const version = (value["version"] as { total?: number } | undefined)?.total;
    const status = (value["status"] as { value?: string } | undefined)?.value;
    docAuthorEl.textContent = author ?? "—";
    docVersionEl.textContent = String(version ?? 0);
    docStatusEl.textContent = status ?? "draft";
  };

  docMeta.onChange(renderDocMeta);
  renderDocMeta(docMeta.value());

  // Set author on connect
  client.waitForConnected().then(() => {
    docMeta.lwwSet("author", myName);
  }).catch(() => {});

  // Increment version counter
  docVersionBtn.addEventListener("click", () => {
    docMeta.incrementCounter("version");
  });

  // Change status
  docStatusSelect.addEventListener("change", () => {
    docMeta.lwwSet("status", docStatusSelect.value);
  });
};
