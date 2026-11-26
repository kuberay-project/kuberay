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

def pr_numbers(rev_range):
    """PR numbers in the commit messages of rev_range, with squashed cherry-pick commits expanded."""
    log = run("git", "log", rev_range, "--format=%B%x00")
    numbers = set()
    for msg in (m.strip() for m in log.split("\0")):
        numbers |= {int(n) for n in re.findall(r"\(#(\d+)\)", msg)}
        # Cherry-pick PR bodies list the original PRs as "- #N", not "(#N)".
        if "cherry" in msg.split("\n", 1)[0].lower():
            numbers |= {int(n) for n in re.findall(r"^\s*[-*]\s*#(\d+)", msg, re.M)}
    return numbers

prev, new = sys.argv[1:3]
# A PR cherry-picked to the previous release branch has a different commit there than on master, so
# `prev..new` still lists it. Its number in the release branch's cherry-pick commits tells us it already shipped.
# For a patch release, prev is an ancestor of new, the range is empty, and nothing is marked.
merge_base = run("git", "merge-base", prev, new).strip()
released = pr_numbers(f"{merge_base}..{prev}")

out = []
for n in sorted(pr_numbers(f"{prev}..{new}")):
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
        "already_released": n in released,
    })
print(json.dumps(out, indent=1))
