// The cluster selection page: lists the sessions from GET /clusters, enters one
// through /enter_cluster, and then opens the Ray dashboard on the same origin.
"use strict";

// ---- Theme ----

// Same rules and localStorage key as the Ray dashboard (src/App.tsx), so the
// choice carries over on their shared origin: ?theme=, then the stored choice,
// then the system theme.
const THEME_KEY = "themeMode";
// Kept in a variable: a MediaQueryList that nothing references can be
// garbage-collected along with its change listener.
const darkQuery = window.matchMedia("(prefers-color-scheme: dark)");

const isThemeMode = (value) => value === "light" || value === "dark";
const urlTheme = new URLSearchParams(window.location.search).get("theme");
// ?theme= fixes the mode, so the page has no toggle then.
const themeFixed = isThemeMode(urlTheme);

// localStorage throws when the browser blocks site data. The page then follows
// the system theme, and a picked theme isn't kept.
const readStoredTheme = () => {
    try {
        return localStorage.getItem(THEME_KEY);
    } catch {
        return null;
    }
};

const storeTheme = (value) => {
    try {
        localStorage.setItem(THEME_KEY, value);
    } catch {
        // Storage is blocked or full: the theme still changes, it just isn't remembered.
    }
};

const pickTheme = () => {
    const storedTheme = readStoredTheme();
    return themeFixed ? urlTheme : isThemeMode(storedTheme) ? storedTheme : darkQuery.matches ? "dark" : "light";
};

let theme = pickTheme();

const applyTheme = () => {
    document.documentElement.dataset.theme = theme;
};

// Runs before the body is parsed, so the page is never drawn in the wrong theme.
applyTheme();

// ---- Sessions ----

// sessionName of a running cluster; any other value is a stored session.
const LIVE_SESSION = "live";

// The Ray dashboard served on the same origin by `ray start --proxy-server-url`.
const DASHBOARD_URL = "/#/overview";

// Same page size as the Ray dashboard's tables.
const PAGE_SIZE = 10;

const isLive = (c) => c.sessionName === LIVE_SESSION;

const sessionId = (c) => `${c.namespace}/${c.name}/${c.sessionName}`;

// A link that enters the session on load, so it also works in a new tab. It
// keeps the page's other parameters, such as ?theme=.
const sessionHref = (c) => {
    const params = new URLSearchParams(window.location.search);
    params.set("namespace", c.namespace);
    params.set("name", c.name);
    params.set("session", c.sessionName);
    return `?${params}`;
};

// The session that a sessionHref link names, read from location.search.
const linkedSession = (search) => {
    const params = new URLSearchParams(search);
    const session = { namespace: params.get("namespace"), name: params.get("name"), sessionName: params.get("session") };
    // Each value is a path segment of /enter_cluster, which "." and ".." would leave.
    return Object.values(session).every((value) => value && value !== "." && value !== "..") ? session : undefined;
};

// The History Server reports owner kinds in lower case.
const OWNER_KINDS = { raycluster: "RayCluster", rayjob: "RayJob", rayservice: "RayService" };

const ownerKindLabel = (kind) => OWNER_KINDS[kind.toLowerCase()] ?? kind;

const hasStatus = (c, status) => status === "all" || (status === "live") === isLive(c);

const matches = (c, query) => {
    const lowerQuery = query.toLowerCase();
    return [c.name, c.namespace, c.sessionName, c.ownerKind, c.ownerName].some((field) => (field ?? "").toLowerCase().includes(lowerQuery));
};

const SORT_VALUES = {
    name: (c) => c.name,
    namespace: (c) => c.namespace,
    // Ascending puts live sessions first.
    status: (c) => (isLive(c) ? 0 : 1),
    owner: (c) => c.ownerName ?? "",
    createdAt: (c) => c.createTimeStamp,
};

const compareBy = (sort) => {
    const sortValue = SORT_VALUES[sort.key];
    const sign = sort.order === "asc" ? 1 : -1;
    return (a, b) => {
        const x = sortValue(a);
        const y = sortValue(b);
        const order = typeof x === "number" && typeof y === "number" ? x - y : String(x).localeCompare(String(y));
        return sign * order;
    };
};

const range = (start, end) => Array.from({ length: end - start + 1 }, (_, i) => start + i);

// The buttons of MUI's Pagination with one boundary and one sibling page:
// "previous", page numbers, "start-ellipsis", "end-ellipsis" and "next".
const paginationItems = (page, count) => {
    const siblingsStart = Math.max(Math.min(page - 1, count - 4), 3);
    const siblingsEnd = Math.min(Math.max(page + 1, 5), count - 2);
    return [
        "previous",
        1,
        ...(siblingsStart > 3 ? ["start-ellipsis"] : count > 3 ? [2] : []),
        ...range(siblingsStart, siblingsEnd),
        ...(siblingsEnd < count - 2 ? ["end-ellipsis"] : count > 2 ? [count - 1] : []),
        ...(count > 1 ? [count] : []),
        "next",
    ];
};

// ---- History Server API ----

const statusError = (res) => new Error(`${res.status}: ${res.statusText || "Error"}`);

const fetchClusters = async () => {
    const res = await fetch("/clusters");
    if (!res.ok) {
        throw statusError(res);
    }
    return (await res.json()) ?? [];
};

// Sets cookies that route the Ray dashboard's requests to the session. Every
// failure is a non-2xx status, so a 2xx means done and its body isn't read.
const enterCluster = async ({ namespace, name, sessionName }) => {
    // A link can name the session, so encode each value to keep it in its path segment.
    const path = [namespace, "raycluster", name, sessionName].map(encodeURIComponent).join("/");
    const res = await fetch(`/enter_cluster/${path}`);
    if (!res.ok) {
        const body = (await res.text()).trim();
        throw body ? new Error(body) : statusError(res);
    }
};

// ---- Page ----

// How long an enter error shows; it pauses on hover or in the background.
const ENTER_ERROR_MS = 6000;

const STATUS_NAMES = { all: "", live: "live ", ended: "ended " };

const state = {
    // "loading", "loaded" with clusters, or "failed" with message.
    sessions: { status: "loading" },
    query: "",
    statusFilter: "all",
    // Newest first, the order the History Server returns.
    sort: { key: "createdAt", order: "desc" },
    page: 1,
    // sessionId being opened; one at a time.
    entering: null,
};

const byId = (id) => document.getElementById(id);

const element = (tag, className, text) => {
    const el = document.createElement(tag);
    if (className) {
        el.className = className;
    }
    if (text !== undefined) {
        el.textContent = text;
    }
    return el;
};

// The filtered, sorted page of clusters that the table shows.
const sessionList = () => {
    const clusters = state.sessions.status === "loaded" ? state.sessions.clusters : [];
    const matching = clusters.filter((c) => hasStatus(c, state.statusFilter) && matches(c, state.query)).sort(compareBy(state.sort));
    const pageCount = Math.max(1, Math.ceil(matching.length / PAGE_SIZE));
    // A refresh can leave fewer pages than the one shown.
    const page = Math.min(state.page, pageCount);
    const first = (page - 1) * PAGE_SIZE;
    return { total: clusters.length, matchingCount: matching.length, page, pageCount, first, rows: matching.slice(first, first + PAGE_SIZE) };
};

const renderThemeToggle = () => {
    const toggle = byId("theme-toggle");
    const label = theme === "light" ? "Dark mode" : "Light mode";
    toggle.hidden = themeFixed;
    toggle.dataset.tooltip = label;
    toggle.setAttribute("aria-label", label);
};

const sessionRow = (cluster) => {
    const row = byId("session-row").content.firstElementChild.cloneNode(true);
    const [nameCell, namespaceCell, statusCell, ownerCell, createdCell] = row.cells;
    const busy = state.entering !== null;

    const link = nameCell.querySelector(".link");
    link.textContent = cluster.name;
    link.dataset.session = sessionId(cluster);
    if (busy) {
        // No href while busy, so no new tabs either; role keeps it a link.
        link.setAttribute("role", "link");
        link.setAttribute("aria-disabled", "true");
    } else {
        link.href = sessionHref(cluster);
    }
    link.addEventListener("click", (event) => {
        // Leave middle and modified clicks (new tab or window) to the browser.
        if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) {
            return;
        }
        event.preventDefault();
        enter(cluster);
    });
    if (state.entering === sessionId(cluster)) {
        const spinner = nameCell.querySelector(".spinner");
        spinner.removeAttribute("hidden");
        spinner.setAttribute("aria-label", `Opening ${cluster.name}`);
    }

    namespaceCell.textContent = cluster.namespace || "-";

    if (isLive(cluster)) {
        statusCell.append(element("span", "chip success", "Live"));
    } else {
        const sessionName = element("span", "session-id");
        sessionName.tabIndex = 0;
        sessionName.dataset.tooltip = cluster.sessionName;
        sessionName.setAttribute("aria-description", cluster.sessionName);
        sessionName.append(element("span", "chip inactive", "Ended"));
        statusCell.append(sessionName);
    }

    if (cluster.ownerKind || cluster.ownerName) {
        const owner = element("span", "owner", cluster.ownerName || "-");
        if (cluster.ownerKind) {
            owner.append(element("span", "chip", ownerKindLabel(cluster.ownerKind)));
        }
        ownerCell.append(owner);
    } else {
        ownerCell.textContent = "-";
    }

    createdCell.textContent = cluster.createTime || "-";
    return row;
};

// What an empty session table says, depending on why it is empty.
const emptyRow = (hasSessions) => {
    const row = byId("empty-row").content.firstElementChild.cloneNode(true);
    const status = STATUS_NAMES[state.statusFilter];
    if (!hasSessions) {
        row.querySelector(".empty-title").textContent = "No cluster sessions yet";
        row.querySelector(".empty-detail").textContent = "Sessions appear here after a Ray cluster runs with the History Server collector.";
    } else {
        row.querySelector(".empty-title").textContent = "No matching sessions";
        row.querySelector(".empty-detail").textContent = state.query ? `No ${status}cluster or session matches "${state.query}".` : `There are no ${status}sessions.`;
    }
    return row;
};

const pageButton = (item, page, pageCount) => {
    if (item === "start-ellipsis" || item === "end-ellipsis") {
        return element("span", "page-item page-ellipsis", "…");
    }
    const button = element("button", "page-item");
    button.type = "button";
    if (item === "previous" || item === "next") {
        const [previousArrow, nextArrow] = byId("page-arrows").content.children;
        button.append((item === "previous" ? previousArrow : nextArrow).cloneNode(true));
        button.setAttribute("aria-label", `Go to ${item} page`);
        button.disabled = item === "previous" ? page === 1 : page === pageCount;
        button.addEventListener("click", () => goToPage(page + (item === "previous" ? -1 : 1), button.getAttribute("aria-label")));
    } else {
        button.textContent = item;
        if (item === page) {
            button.setAttribute("aria-current", "page");
            button.setAttribute("aria-label", `page ${item}`);
        } else {
            button.setAttribute("aria-label", `Go to page ${item}`);
        }
        button.addEventListener("click", () => goToPage(item, `page ${item}`));
    }
    return button;
};

const render = () => {
    const { sessions } = state;
    byId("refresh").disabled = sessions.status === "loading";
    byId("entering").hidden = state.entering === null;
    byId("clear-search").hidden = !state.query;
    // An <svg> has no hidden property, only the attribute.
    byId("search-icon").toggleAttribute("hidden", Boolean(state.query));
    for (const toggle of document.querySelectorAll(".toggle")) {
        toggle.setAttribute("aria-pressed", String(toggle.dataset.status === state.statusFilter));
    }
    for (const label of document.querySelectorAll(".sort-label")) {
        const th = label.parentElement;
        if (label.dataset.sort === state.sort.key) {
            th.setAttribute("aria-sort", state.sort.order === "asc" ? "ascending" : "descending");
        } else {
            th.removeAttribute("aria-sort");
        }
    }

    byId("loading").hidden = sessions.status !== "loading";
    byId("load-error").hidden = sessions.status !== "failed";
    byId("load-error").querySelector(".alert-message").textContent = sessions.message ?? "";
    byId("sessions").hidden = sessions.status !== "loaded";
    if (sessions.status === "loaded") {
        const list = sessionList();
        // Stay on the page shown, so a later refresh with more pages doesn't jump back.
        state.page = list.page;
        byId("session-rows").replaceChildren(...(list.rows.length ? list.rows.map(sessionRow) : [emptyRow(list.total > 0)]));
        const shown = list.rows.length === 0 ? 0 : `${list.first + 1}–${list.first + list.rows.length}`;
        const total = list.matchingCount < list.total ? ` (${list.total} in total)` : "";
        byId("showing").textContent = `Showing ${shown} of ${list.matchingCount} sessions${total}`;
        byId("pagination").replaceChildren(...(list.pageCount > 1 ? paginationItems(list.page, list.pageCount).map((item) => pageButton(item, list.page, list.pageCount)) : []));
    }
    updateTooltip();
};

// Any change to the listing goes back to page 1.
const changeListing = (change) => {
    Object.assign(state, change, { page: 1 });
    render();
};

// Rendering rebuilds the page buttons, so the focus goes back to the button
// with focusLabel, or to the current page once that button is disabled.
const goToPage = (page, focusLabel) => {
    state.page = page;
    render();
    const pagination = byId("pagination");
    (pagination.querySelector(`[aria-label="${focusLabel}"]:enabled`) ?? pagination.querySelector("[aria-current=page]")).focus();
};

const load = async () => {
    try {
        state.sessions = { status: "loaded", clusters: await fetchClusters() };
    } catch (err) {
        state.sessions = { status: "failed", message: err.message };
    }
    render();
};

const refresh = () => {
    state.sessions = { status: "loading" };
    render();
    load();
};

// ---- Entering a session ----

let enterErrorTimer;

const startEnterErrorTimer = (ms) => {
    clearTimeout(enterErrorTimer);
    enterErrorTimer = setTimeout(hideEnterError, ms);
};

const showEnterError = (message) => {
    byId("enter-error").querySelector(".alert-message").textContent = message;
    byId("enter-error").hidden = false;
    startEnterErrorTimer(ENTER_ERROR_MS);
};

const hideEnterError = () => {
    clearTimeout(enterErrorTimer);
    byId("enter-error").hidden = true;
};

// Like MUI's Snackbar, the timer restarts with half the time.
const resumeEnterError = () => {
    if (!byId("enter-error").hidden) {
        startEnterErrorTimer(ENTER_ERROR_MS / 2);
    }
};

// Resolves to whether the session was entered.
const finishEnter = async (session) => {
    try {
        await enterCluster(session);
        window.location.href = DASHBOARD_URL;
        return true;
    } catch (err) {
        showEnterError(err.message);
        state.entering = null;
        render();
        return false;
    }
};

const enter = async (session) => {
    if (state.entering) {
        return;
    }
    const id = sessionId(session);
    state.entering = id;
    hideEnterError();
    render();
    // Rendering rebuilt the rows and dropped the focus, so give it back to the
    // row's link unless the user has moved it meanwhile.
    if (!(await finishEnter(session)) && document.activeElement === document.body) {
        byId("session-rows").querySelector(`[data-session="${CSS.escape(id)}"]`)?.focus();
    }
};

// ---- Tooltip ----

// One tooltip, shown below whichever [data-tooltip] element is hovered or focused.
let tooltipAnchor = null;

const placeTooltip = () => {
    const tooltip = byId("tooltip");
    tooltip.textContent = tooltipAnchor.dataset.tooltip;
    tooltip.hidden = false;
    const anchor = tooltipAnchor.getBoundingClientRect();
    // offsetWidth, unlike the bounding box, ignores the grow animation's scale.
    const width = tooltip.offsetWidth;
    const left = anchor.left + anchor.width / 2 - width / 2;
    tooltip.style.left = `${Math.max(8, Math.min(left, document.documentElement.clientWidth - width - 8))}px`;
    tooltip.style.top = `${anchor.bottom + 14}px`;
};

const showTooltip = (target) => {
    if (target === tooltipAnchor) {
        return;
    }
    tooltipAnchor = target;
    if (target) {
        placeTooltip();
    } else {
        byId("tooltip").hidden = true;
    }
};

// A render can change the anchor's text or remove it.
const updateTooltip = () => {
    if (tooltipAnchor?.isConnected) {
        placeTooltip();
    } else {
        showTooltip(null);
    }
};

// ---- Wiring ----

document.addEventListener("DOMContentLoaded", () => {
    renderThemeToggle();
    byId("theme-toggle").addEventListener("click", () => {
        theme = theme === "light" ? "dark" : "light";
        storeTheme(theme);
        applyTheme();
        renderThemeToggle();
        updateTooltip();
    });
    const repickTheme = () => {
        theme = pickTheme();
        applyTheme();
        renderThemeToggle();
    };
    // The system theme applies only while the user hasn't picked one.
    darkQuery.addEventListener("change", repickTheme);

    byId("refresh").addEventListener("click", refresh);
    byId("search").addEventListener("input", (event) => changeListing({ query: event.target.value }));
    byId("clear-search").addEventListener("click", () => {
        byId("search").value = "";
        changeListing({ query: "" });
        byId("search").focus();
    });
    for (const toggle of document.querySelectorAll(".toggle")) {
        // Clicking the selected button keeps it selected.
        toggle.addEventListener("click", () => changeListing({ statusFilter: toggle.dataset.status }));
    }
    for (const label of document.querySelectorAll(".sort-label")) {
        // Like the Ray dashboard: a new column sorts ascending, the same column flips.
        label.addEventListener("click", () => {
            const key = label.dataset.sort;
            const order = state.sort.key === key && state.sort.order === "asc" ? "desc" : "asc";
            changeListing({ sort: { key, order } });
        });
    }

    // Only the timer or the close button dismisses an enter error.
    const enterError = byId("enter-error");
    byId("close-enter-error").addEventListener("click", hideEnterError);
    enterError.addEventListener("mouseenter", () => clearTimeout(enterErrorTimer));
    enterError.addEventListener("mouseleave", resumeEnterError);
    window.addEventListener("blur", () => clearTimeout(enterErrorTimer));
    window.addEventListener("focus", resumeEnterError);

    document.addEventListener("pointerover", (event) => showTooltip(event.target.closest("[data-tooltip]")));
    document.addEventListener("focusin", (event) => showTooltip(event.target.closest("[data-tooltip]")));
    document.addEventListener("focusout", () => showTooltip(null));
    // The pointer left the window.
    document.addEventListener("pointerout", (event) => event.relatedTarget || showTooltip(null));
    window.addEventListener("scroll", () => showTooltip(null), true);

    // Going back can restore this page from the bfcache as it was left. Clear the
    // stale spinner, and pick up a theme picked in the Ray dashboard and sessions
    // that changed meanwhile; the old list shows until the new one loads.
    window.addEventListener("pageshow", (event) => {
        if (event.persisted) {
            state.entering = null;
            repickTheme();
            render();
            load();
        }
    });

    const linked = linkedSession(window.location.search);
    if (linked) {
        // Drop the session's parameters so going back or reloading doesn't enter
        // again, but keep any others, such as ?theme=.
        const url = new URL(window.location.href);
        for (const key of ["namespace", "name", "session"]) {
            url.searchParams.delete(key);
        }
        window.history.replaceState(null, "", url);
        state.entering = sessionId(linked);
        finishEnter(linked);
    }
    refresh();
});
