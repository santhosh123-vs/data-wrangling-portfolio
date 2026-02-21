import pandas as pd
import numpy as np
import random
import json
from datetime import datetime, timedelta

def generate_jira_bugs(num_records=2000):
    bug_titles = [
        "Login button not responding on mobile",
        "Database timeout on user search",
        "Payment gateway returns 500 error",
        "UI overlapping on dashboard widgets",
        "Memory leak in background service",
        "API rate limiter not working",
        "CSS broken on Safari browser",
        "Null pointer exception in auth module",
        "File upload fails for PDF files",
        "Email notifications not being sent",
        "Search results return wrong data",
        "Session timeout too short",
        "Password reset link expired instantly",
        "Chart rendering fails with large datasets",
        "Mobile app crashes on startup",
        "Slow query on reports page",
        "User permissions not applied correctly",
        "Export to CSV produces empty file",
        "Timezone conversion incorrect",
        "Form validation bypassed on submit"
    ]
    priorities = [
        "Critical", "CRITICAL", "P0", "Blocker",
        "High", "HIGH", "P1", "Major",
        "Medium", "MEDIUM", "P2", "Normal",
        "Low", "LOW", "P3", "Minor", "Trivial",
        None
    ]
    statuses = [
        "Open", "OPEN", "New", "TODO",
        "In Progress", "IN_PROGRESS", "InProgress", "Working",
        "Resolved", "RESOLVED", "Fixed", "Done",
        "Closed", "CLOSED", "Verified", "Complete",
        "Reopened", "REOPENED", "Re-opened",
        None
    ]
    components = [
        "auth-service", "Authentication", "AUTH",
        "payment-gateway", "Payment", "PAYMENT",
        "user-dashboard", "Dashboard", "UI",
        "api-gateway", "API", "Backend",
        "database", "Database", "DB",
        "notification-service", "Notifications", "EMAIL",
        None
    ]
    reporters = [
        "john.doe@company.com",
        "jane.smith@company.com",
        "qa.team@company.com",
        "dev.lead@company.com",
        "product@company.com",
        "UNKNOWN",
        "",
        None
    ]
    environments = [
        "Production", "PROD", "production", "prod",
        "Staging", "STAGE", "staging", "stg",
        "Development", "DEV", "development", "dev",
        "QA", "Testing", "UAT",
        None
    ]
    sdlc_phases = [
        "Requirements", "Design", "Development",
        "Testing", "Deployment", "Maintenance",
        "requirements", "DESIGN", "Dev", "QA",
        None
    ]
    base_time = datetime(2024, 1, 1)
    bugs = []
    for i in range(num_records):
        created = base_time + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        resolved = created + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23)
        )
        date_formats = [
            created.strftime("%Y-%m-%d %H:%M:%S"),
            created.strftime("%m/%d/%Y %H:%M"),
            created.strftime("%d-%b-%Y %H:%M:%S"),
            str(created.timestamp()),
            None
        ]
        if random.random() < 0.02:
            chosen_date = "INVALID_DATE"
        else:
            chosen_date = random.choice(date_formats)
        resolved_formats = [
            resolved.strftime("%Y-%m-%d %H:%M:%S"),
            resolved.strftime("%m/%d/%Y"),
            None,
            "N/A",
            "Not Yet"
        ]
        time_spent_options = [
            random.randint(1, 480),
            round(random.uniform(0.5, 40.0), 1),
            -1,
            0,
            "N/A",
            None,
            "2 hours",
            "1 day"
        ]
        bug = {
            "ticket_id": "JIRA-{}".format(1000 + i),
            "title": random.choice(bug_titles),
            "description": "Bug found in {} module.".format(
                random.choice(["auth", "payment", "dashboard", "api", "db"])
            ),
            "priority": random.choice(priorities),
            "status": random.choice(statuses),
            "component": random.choice(components),
            "reporter": random.choice(reporters),
            "assignee": random.choice(reporters),
            "environment": random.choice(environments),
            "sdlc_phase": random.choice(sdlc_phases),
            "created_date": chosen_date,
            "resolved_date": random.choice(resolved_formats),
            "time_spent_minutes": random.choice(time_spent_options),
            "browser": random.choice([
                "Chrome", "Firefox", "Safari", "Edge",
                "chrome", "FIREFOX", "IE", None
            ]),
            "os": random.choice([
                "Windows 10", "macOS", "Linux", "iOS", "Android",
                "Win10", "Mac", "ubuntu", None
            ])
        }
        bugs.append(bug)
    num_duplicates = int(num_records * 0.06)
    duplicates = random.choices(bugs, k=num_duplicates)
    bugs.extend(duplicates)
    random.shuffle(bugs)
    return pd.DataFrame(bugs)


def generate_github_issues(num_records=1500):
    labels_options = [
        ["bug"], ["bug", "critical"], ["bug", "high-priority"],
        ["bug", "low-priority"], ["enhancement"], ["bug", "ui"],
        ["bug", "backend"], ["bug", "security"],
        ["bug", "performance"], ["wontfix"],
        [], None
    ]
    states = [
        "open", "closed",
        "OPEN", "CLOSED",
        None
    ]
    titles = [
        "Fix: Login page crash on submit",
        "Bug: API returns 404 for valid endpoints",
        "Issue: Dashboard loading slowly",
        "Error in payment processing module",
        "UI: Button alignment broken on mobile",
        "Backend: Database connection pool exhausted",
        "Security: XSS vulnerability in search",
        "Performance: Report generation timeout",
        "Bug: Email template rendering incorrectly",
        "Fix: Cron job failing silently"
    ]
    base_time = datetime(2024, 1, 1)
    issues = []
    for i in range(num_records):
        created = base_time + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23)
        )
        closed = created + timedelta(
            days=random.randint(1, 60)
        )
        issue = {
            "issue_number": i + 1,
            "title": random.choice(titles),
            "body": "## Description\n{}\n\n## Steps\n{}".format(
                random.choice([
                    "This bug causes the app to crash",
                    "Unexpected behavior in production",
                    "Found during QA testing",
                    "Customer reported this issue",
                    "",
                    None
                ]),
                random.choice([
                    "1. Open app\n2. Click button\n3. See error",
                    "Cannot reproduce consistently",
                    "See screenshot attached",
                    ""
                ])
            ),
            "labels": random.choice(labels_options),
            "state": random.choice(states),
            "user": random.choice([
                "developer1", "qa_tester", "product_manager",
                "customer_support", None, ""
            ]),
            "assignee": random.choice([
                "dev_john", "dev_jane", "dev_bob",
                None, "", "unassigned"
            ]),
            "created_at": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "closed_at": closed.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ) if random.random() > 0.4 else None,
            "comments": random.randint(0, 50),
            "milestone": random.choice([
                "v1.0", "v1.1", "v2.0", "Sprint 5",
                "Sprint 10", "Backlog", None
            ])
        }
        issues.append(issue)
    return issues


def generate_excel_bugs(num_records=1000):
    severities = [
        "Very High", "very high", "V. High",
        "High", "high", "Hi",
        "Medium", "medium", "Med", "med",
        "Low", "low", "Lo",
        "Not Important", "N/A",
        None
    ]
    status_notes = [
        "Fixed by John on Monday",
        "Still working on it",
        "Cant reproduce",
        "DONE",
        "need more info",
        "waiting for deploy",
        "closed - duplicate",
        "wont fix",
        "Fixed",
        "Open",
        None,
        ""
    ]
    bugs = []
    for i in range(num_records):
        bug = {
            "Bug #": i + 1,
            "Bug Title": random.choice([
                "login broken",
                "page not loading",
                "error on submit",
                "slow performance",
                "ui looks weird",
                "data not saving",
                "crash on mobile",
                "wrong calculation",
                "missing button",
                "timeout error"
            ]),
            "Severity": random.choice(severities),
            "Status Notes": random.choice(status_notes),
            "Found By": random.choice([
                "QA Team", "Developer", "Customer",
                "PM", "Auto Test", None, ""
            ]),
            "Date Found": random.choice([
                "Jan 15, 2024",
                "2024-02-20",
                "3/15/24",
                "April 2024",
                "last week",
                "yesterday",
                None,
                ""
            ]),
            "Module": random.choice([
                "Login", "Dashboard", "Payments",
                "API", "Database", "UI",
                "Auth", "Reports", None
            ]),
            "Notes": random.choice([
                "Happens sometimes",
                "Critical for release",
                "Low priority",
                "Customer complaint",
                "Found in sprint 5",
                "",
                None
            ])
        }
        bugs.append(bug)
    return pd.DataFrame(bugs)


if __name__ == "__main__":
    print("Generating messy bug report data...")
    print("")
    print("1. Generating JIRA-style bugs...")
    jira_df = generate_jira_bugs(2000)
    jira_df.to_csv("raw_data/jira_bugs_export.csv", index=False)
    print("   Saved {} records".format(len(jira_df)))
    print("2. Generating GitHub Issues-style bugs...")
    github_issues = generate_github_issues(1500)
    with open("raw_data/github_issues.json", "w") as f:
        json.dump(github_issues, f, indent=2, default=str)
    print("   Saved {} records".format(len(github_issues)))
    print("3. Generating Excel-style bug tracker...")
    excel_df = generate_excel_bugs(1000)
    excel_df.to_excel("raw_data/manual_bug_tracker.xlsx", index=False)
    excel_df.to_csv("raw_data/manual_bug_tracker.csv", index=False)
    print("   Saved {} records".format(len(excel_df)))
    print("")
    print("All data generated successfully!")
    print("")
    print("JIRA sample:")
    print(jira_df.head(3))
    print("")
    print("Excel sample:")
    print(excel_df.head(3))
