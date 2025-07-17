from reports.weekly_summary import load_last_7_days, summarize_jobs, render_template

def run_summary_tool(user=None, start_date=None, end_date=None):
    df = load_last_7_days()

    if user:
        df = df[df["User"] == user]

    summary = summarize_jobs(df)
    markdown = render_template(summary)
    return markdown
