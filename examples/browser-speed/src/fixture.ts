import { Schema } from "effect";

import { Board, seed } from "./contract.ts";

/** A network-free fixture. Its private ledger is read only by the host verifier, never a Tool. */
export const fixtureHtml =
  () => `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Fieldwork · Tasks</title><style>
*{box-sizing:border-box}body{margin:0;background:#f6f6f3;color:#252c2a;font:14px system-ui,sans-serif}header{height:64px;border-bottom:1px solid #dddeda;display:flex;align-items:center;gap:12px;padding:0 32px;background:white}header strong{font-size:18px;letter-spacing:-.7px}header span{color:#829086}main{padding:38px 32px}h1{font-size:30px;letter-spacing:-1px;margin:0 0 8px}p{color:#788078;margin:0 0 28px}.toolbar{display:flex;gap:10px;margin:24px 0}button,input,select{font:inherit;border:1px solid #d9ddd7;border-radius:6px;background:white;padding:10px 12px;color:inherit}button{cursor:pointer}button:focus-visible,input:focus-visible,select:focus-visible{outline:3px solid #97bd8e;outline-offset:2px}button.primary{background:#284e38;color:white;border-color:#284e38}.toolbar input{flex:1}.toolbar .primary{margin-left:auto}table{width:100%;border-collapse:collapse;background:white;border:1px solid #e0e4dc}th{text-align:left;color:#79837a;font-size:11px;text-transform:uppercase;letter-spacing:1px;background:#f1f3ed;padding:14px}td{padding:16px 14px;border-top:1px solid #e8eae5}td:first-child{font-weight:550}.pill{background:#edf1e9;border-radius:20px;padding:5px 9px;font-size:12px}.high{background:#fae9db;color:#975b28}dialog{width:420px;border:1px solid #d9ddd7;border-radius:12px;padding:26px;box-shadow:0 18px 70px #152a2333}dialog::backdrop{background:#132b2038}h2{margin:0 0 24px;font-size:23px}label{display:grid;gap:7px;margin:15px 0;color:#636f65}label input,label select{width:100%;color:#252c2a}.actions{display:flex;justify-content:flex-end;gap:8px;margin-top:26px}#notice{height:20px;color:#315d37;margin-top:20px;font-size:13px}footer{font-size:11px;color:#8c968d;margin-top:28px}
</style></head><body><header><strong>◈ Fieldwork</strong><span>/</span><span>Workspace</span></header><main><h1>Tasks</h1><p>A little less planning. A little more doing.</p><div class="toolbar"><input id="search" aria-label="Search tasks" placeholder="Search tasks…"><select id="filter" aria-label="Filter by assignee"><option value="">Everyone</option><option>Alex</option><option>Sam</option><option>Jordan</option></select><button id="new-task" class="primary">+ New task</button></div><table><thead><tr><th>Task</th><th>Assignee</th><th>Priority</th><th>Status</th><th></th></tr></thead><tbody id="rows"></tbody></table><div id="notice" role="status"></div><footer>FIELDWORK / BROWSER SPEED LAB · Synthetic data</footer></main><dialog id="editor"><form id="task-form"><h2 id="dialog-title">Create task</h2><label>Title<input id="title" name="title" required maxlength="120"></label><label>Assignee<select id="assignee" name="assignee"><option>Alex</option><option>Sam</option><option>Jordan</option></select></label><label>Priority<select id="priority" name="priority"><option>Low</option><option selected>Medium</option><option>High</option></select></label><label>Status<select id="status" name="status"><option>Todo</option><option>Doing</option><option>Done</option></select></label><div class="actions"><button id="cancel" type="button">Cancel</button><button id="save" class="primary" type="submit">Save task</button></div></form></dialog><script id="board-state" type="application/json">${Schema.encodeSync(Schema.fromJsonString(Board))(seed).replaceAll("<", "\\u003c")}</script><script>
(() => {
  const ledger = document.getElementById('board-state');
  const tasks = JSON.parse(ledger.textContent);
  const field = id => document.getElementById(id);
  const dialog = field('editor');
  let editing = null;
  const render = () => {
    const search = field('search').value.toLowerCase();
    const assignee = field('filter').value;
    const rows = field('rows');
    rows.replaceChildren();
    for (const task of tasks.filter(t => t.title.toLowerCase().includes(search) && (!assignee || t.assignee === assignee))) {
      const row = document.createElement('tr');
      for (const key of ['title', 'assignee', 'priority', 'status']) {
        const cell = document.createElement('td');
        const content = document.createElement('span');
        content.textContent = task[key];
        if (key === 'priority' || key === 'status') content.className = 'pill' + (task[key] === 'High' ? ' high' : '');
        cell.append(content); row.append(cell);
      }
      const cell = document.createElement('td');
      const button = document.createElement('button');
      button.id = 'edit-' + task.id; button.textContent = 'Edit'; button.setAttribute('aria-label', 'Edit ' + task.title);
      button.onclick = () => open(task); cell.append(button); row.append(cell); rows.append(row);
    }
    ledger.textContent = JSON.stringify(tasks);
  };
  const open = task => {
    editing = task ? task.id : null;
    field('dialog-title').textContent = task ? 'Edit task' : 'Create task';
    field('title').value = task ? task.title : '';
    field('assignee').value = task ? task.assignee : 'Alex';
    field('priority').value = task ? task.priority : 'Medium';
    field('status').value = task ? task.status : 'Todo';
    dialog.showModal();
  };
  field('new-task').onclick = () => open(null);
  field('cancel').onclick = () => dialog.close();
  field('search').oninput = render; field('filter').onchange = render;
  field('task-form').onsubmit = event => {
    event.preventDefault();
    const title = field('title').value.trim();
    if (!title || (editing === null && tasks.length >= 50)) return;
    const task = {id: editing === null ? Math.max(0, ...tasks.map(t => t.id)) + 1 : editing, title, assignee: field('assignee').value, priority: field('priority').value, status: field('status').value};
    if (editing === null) tasks.push(task); else tasks.splice(tasks.findIndex(t => t.id === editing), 1, task);
    dialog.close(); render(); field('notice').textContent = 'Saved ' + title;
  };
  render();
})();
</script></body></html>`;
