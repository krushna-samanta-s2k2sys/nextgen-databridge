"""
Reads version manifests from the PARAMS env var, formats a Markdown summary,
and appends it to $GITHUB_STEP_SUMMARY.
Called by .github/workflows/list-versions.yml.
"""
import json
import os


def main():
    params_json = os.environ.get("PARAMS", "[]")
    count       = int(os.environ.get("COUNT", "10"))
    dev_ver     = os.environ.get("DEV_VER", "—")
    staging_ver = os.environ.get("STAGING_VER", "—")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")

    params = json.loads(params_json)
    manifests = []
    for raw in params:
        try:
            manifests.append(json.loads(raw))
        except Exception:
            pass

    manifests.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    lines = [
        "## Available Versions",
        "",
        "| Environment | Live Version |",
        "|---|---|",
        f"| dev     | `{dev_ver}` |",
        f"| staging | `{staging_ver}` |",
        "",
        "### Recent versions — newest first",
        "",
        "| Version ID | Commit | Date (UTC) | Actor | Apps | DAGs | Configs | Run |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for m in manifests[:count]:
        vid     = m.get("version_id", "?")
        sha     = m.get("sha", "?")[:7]
        created = m.get("created_at", "?")[:16].replace("T", " ")
        actor   = m.get("created_by", "?")
        arts    = m.get("artifacts", {})
        apps    = "✓" if arts.get("apps")    else "—"
        dags    = "✓" if arts.get("dags")    else "—"
        cfgs    = "✓" if arts.get("configs") else "—"
        run_url = m.get("run_url", "")
        run_id  = m.get("run_id", "?")
        run_lnk = f"[#{run_id}]({run_url})" if run_url else str(run_id)
        lines.append(
            f"| `{vid}` | `{sha}` | {created} | {actor} | {apps} | {dags} | {cfgs} | {run_lnk} |"
        )

    lines += [
        "",
        "> **To promote:** copy a `version_id` from the table, open "
        "**Actions → Promote → Run workflow**, paste it into the "
        "`version_id` field, and choose the target environment.",
    ]

    output = "\n".join(lines) + "\n"
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(output)
    else:
        print(output)


if __name__ == "__main__":
    main()
