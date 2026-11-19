#!/usr/bin/env python3
"""Usage: collect.py <prev_tag> <new_tag>  ->  JSON array of PR metadata on stdout."""
import json, re, subprocess, sys

def run(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout

def extract_fenced_block(body, lang):
    """Text inside the ```lang fence, "" if the fence is empty, None if the body has no such fence (old PR template)."""
    m = re.search(rf"```{lang}\s*\n(.*?)```", body, re.S)
    return m.group(1).strip() if m else None

def extract_why_paragraph(body):
    """First paragraph of "Why are these changes needed?", with HTML comments removed."""
    m = re.search(r"## Why are these changes needed[^\n]*\n(.*?)(?=\n## |\Z)", body, re.S)
    if not m:
        return ""
    text = re.sub(r"<!--.*?-->", "", m.group(1), flags=re.S).strip()
    return text.split("\n\n")[0]

prev, new = sys.argv[1:3]
# Full commit bodies: a squashed cherry-pick commit lists every original PR as "(#N)".
log = run("git", "log", f"{prev}..{new}", "--format=%B")
numbers = sorted({int(n) for n in re.findall(r"\(#(\d+)\)", log)})

out = []
for n in numbers:
    try:
        p = json.loads(run("gh", "pr", "view", str(n), "--json", "title,labels,body"))
    except subprocess.CalledProcessError as e:
        print(f"skip #{n}: {e.stderr.strip()}", file=sys.stderr)
        continue
    if "cherry" in p["title"].lower():
        continue  # already expanded into its original PRs above
    body = (p["body"] or "").replace("\r\n", "\n")  # GitHub PR bodies use CRLF
    note = extract_fenced_block(body, "release-note")
    out.append({
        "number": n,
        "title": p["title"],
        "labels": [l["name"] for l in p["labels"]],
        "release-note": extract_why_paragraph(body) if note is None else note,
        "breaking-change": extract_fenced_block(body, "breaking-change") or "",
        "source": "why-section" if note is None else "release-note-block",
    })
print(json.dumps(out, indent=1))
