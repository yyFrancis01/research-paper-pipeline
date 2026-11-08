"""
PLACE THIS FILE IN:   <your local project>/dags/paper_pipeline.py

What it does (the numbers match the steps in the assignment PDF):
    3. One scheduled Airflow DAG that looks for new PDFs
    4. Converts each PDF to Markdown (pymupdf)
    5. Asks a local LLM (Ollama, OpenAI-compatible) for title/summary/keywords/category as JSON
    6. Parses the JSON and saves it in a local SQLite database
    7. Regenerates a static HTML page (search + category filter)
    8. Sends a Discord webhook notification

The DAG has 5 tasks that run one after the other:

    find_new_pdfs -> convert_pdfs -> extract_and_store -> generate_html -> notify_discord

All the code is in this single file. It is split into sections:
    SECTION 1: Imports
    SECTION 2: Configuration (paths, LLM, Discord)
    SECTION 3: SQLite database helpers          (step 6)
    SECTION 4: PDF -> Markdown                  (step 4)
    SECTION 5: LLM call + JSON parsing          (step 5)
    SECTION 6: HTML page generation             (step 7)
    SECTION 7: Discord notification             (step 8)
    SECTION 8: The Airflow DAG and its tasks    (step 3)
"""

import json
import os
import sqlite3
from datetime import datetime

import pymupdf
import requests
from openai import OpenAI

# Airflow 3 exposes DAG and task in "airflow.sdk". Airflow 2 uses the old
# locations. This try/except makes the file work with both versions.
try:
    from airflow.sdk import DAG, task
except ImportError:
    from airflow import DAG
    from airflow.decorators import task


# Folder synced by Google Drive, here it works on local file (contains papers/ and output/)
LIBRARY_DIR = os.environ.get("LIBRARY_DIR", os.path.expanduser("~/Google Drive/ResearchLibrary"))
PAPERS_DIR = os.path.join(LIBRARY_DIR, "papers")
OUTPUT_DIR = os.path.join(LIBRARY_DIR, "output")
HTML_PATH = os.path.join(OUTPUT_DIR, "index.html")

#The database lives here
PROJECT_DIR = os.environ.get("PROJECT_DIR", os.path.expanduser("~/paper_project"))
DB_PATH = os.path.join(PROJECT_DIR, "data", "papers.db")
MARKDOWN_DIR = os.path.join(PROJECT_DIR, "data", "markdown")

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "ollama")  # Ollama ignores it, but the client needs one
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.2")
MAX_MARKDOWN_CHARS = 12000
#Change if you want a notification on discord
DISCORD_WEBHOOK_URL = ""

# ============================================================================
# SECTION 3: SQLite database helpers (step 6)
# ============================================================================
def get_connection():
    """
    Open the SQLite database and make sure the 'papers' table exists
    The folder of the database file is created if needed.
    """
    db_folder = os.path.dirname(DB_PATH)
    os.makedirs(db_folder, exist_ok=True)

    connection = sqlite3.connect(DB_PATH)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS papers (
            filename TEXT PRIMARY KEY,
            title    TEXT NOT NULL,
            summary  TEXT NOT NULL,
            keywords TEXT NOT NULL,
            category TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def get_processed_filenames():
    connection = get_connection()
    rows = connection.execute("SELECT filename FROM papers").fetchall()
    connection.close()

    filenames = []
    for row in rows:
        filenames.append(row[0])
    return filenames


def save_paper(filename, metadata):
    keywords_as_json = json.dumps(metadata["keywords"])

    connection = get_connection()
    connection.execute(
        "INSERT OR REPLACE INTO papers (filename, title, summary, keywords, category) "
        "VALUES (?, ?, ?, ?, ?)",
        (filename, metadata["title"], metadata["summary"], keywords_as_json, metadata["category"]),
    )
    connection.commit()
    connection.close()


def get_all_papers():
    """
    Read every paper from the database
    """
    connection = get_connection()
    rows = connection.execute(
        "SELECT filename, title, summary, keywords, category FROM papers ORDER BY title"
    ).fetchall()
    connection.close()

    papers = []
    for row in rows:
        paper = {
            "filename": row[0],
            "title": row[1],
            "summary": row[2],
            "keywords": json.loads(row[3]),
            "category": row[4],
        }
        papers.append(paper)
    return papers


#PDF -> Markdown (step 4)
def convert_pdf_to_markdown(pdf_path):
    markdown_text = pymupdf.to_markdown(pdf_path)
    return markdown_text


#LLM call and JSON parsing (step 5)
def build_prompt(markdown_text):
    short_text = markdown_text[:MAX_MARKDOWN_CHARS]

    prompt = (
        "You are an assistant that catalogues research papers.\n"
        "Read the paper below and answer with ONLY a JSON object, no other text, "
        "with exactly this structure:\n"
        "{\n"
        '  "title": "Paper title",\n'
        '  "summary": "A short summary of the paper (2-4 sentences).",\n'
        '  "keywords": ["keyword one", "keyword two"],\n'
        '  "category": "Data Engineering"\n'
        "}\n"
        '"keywords" must be a list of strings and "category" a single short label.\n\n'
        "PAPER:\n" + short_text
    )
    return prompt


def call_the_T_800(prompt):
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.2,
    )
    return response.choices[0].message.content


def parse_and_check(raw_text):
    """
    Turn the LLM answer into a dict and check the basic structure
    """
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as error:
        raise ValueError("LLM answer not valid JSON: " + str(error))

    if not isinstance(data, dict):
        raise ValueError("LLM answer is JSON but not an object")

    # Text fields: must exist and be strings.
    for field in ["title", "summary", "category"]:
        if field not in data:
            raise ValueError("Missing field: " + field)
        if not isinstance(data[field], str):
            raise ValueError("Field '" + field + "' must be a string")

    # Keywords: must exist and be a list of strings.
    if "keywords" not in data:
        raise ValueError("Missing field: keywords")
    if not isinstance(data["keywords"], list):
        raise ValueError("Field 'keywords' must be a list")
    for keyword in data["keywords"]:
        if not isinstance(keyword, str):
            raise ValueError("Every keyword must be a string")

    return data


def extract_metadata(markdown_text):
    """
    Ask the LLM for the metadata of one paper.

    If the answer cannot be parsed or is invalid, try ONE more time
    (as required by the assignment). If the second try also fails,
    the ValueError is raised to the caller.
    Returns a dict with title, summary, keywords and category.
    """
    prompt = build_prompt(markdown_text)

    try:
        raw_text = call_the_T_800(prompt)
        return parse_and_check(raw_text)
    except ValueError as first_error:
        print("First LLM attempt failed (" + str(first_error) + "). Retrying once...")

    raw_text = call_the_T_800(prompt)
    return parse_and_check(raw_text)


# ============================================================================
# SECTION 6: Static HTML page (step 7)
# ============================================================================
# The page is a template. The placeholder __PAPERS_JSON__ is replaced by the
# list of papers (as JSON). All the searching/filtering is done in the browser
# with a few lines of JavaScript, so no server is needed.
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Research Library</title>
<style>
  body { font-family: Arial, sans-serif; max-width: 900px; margin: 30px auto; padding: 0 15px; }
  h1 { margin-bottom: 5px; }
  .controls { margin: 15px 0; }
  input, select { padding: 8px; font-size: 15px; margin-right: 8px; }
  .paper { border: 1px solid #ccc; border-radius: 6px; padding: 12px; margin-bottom: 12px; }
  .paper h2 { margin: 0 0 5px 0; font-size: 18px; }
  .meta { color: #555; font-size: 13px; margin-bottom: 6px; }
  .keywords span { background: #eef; border-radius: 10px; padding: 2px 8px; margin-right: 4px; font-size: 12px; }
</style>
</head>
<body>
<h1>Research Library</h1>
<div class="controls">
  <input id="search" type="text" placeholder="Search title, summary, keywords...">
  <select id="category"><option value="">All categories</option></select>
  <span id="count"></span>
</div>
<div id="list"></div>

<script>
var papers = __PAPERS_JSON__;

var searchBox = document.getElementById("search");
var categoryBox = document.getElementById("category");
var listDiv = document.getElementById("list");
var countSpan = document.getElementById("count");

// Fill the category drop-down with each distinct category.
function fillCategories() {
  var seen = {};
  for (var i = 0; i < papers.length; i++) {
    var category = papers[i].category;
    if (!seen[category]) {
      seen[category] = true;
      var option = document.createElement("option");
      option.value = category;
      option.textContent = category;
      categoryBox.appendChild(option);
    }
  }
}

// Return true if the paper matches the search text and chosen category.
function paperMatches(paper, searchText, chosenCategory) {
  if (chosenCategory !== "" && paper.category !== chosenCategory) {
    return false;
  }
  var allText = (paper.title + " " + paper.summary + " " + paper.keywords.join(" ")).toLowerCase();
  return allText.indexOf(searchText) !== -1;
}

// Build the HTML elements for one paper (textContent avoids HTML injection).
function createPaperElement(paper) {
  var box = document.createElement("div");
  box.className = "paper";

  var title = document.createElement("h2");
  title.textContent = paper.title;
  box.appendChild(title);

  var meta = document.createElement("div");
  meta.className = "meta";
  meta.textContent = "Category: " + paper.category + "  |  File: " + paper.filename;
  box.appendChild(meta);

  var summary = document.createElement("p");
  summary.textContent = paper.summary;
  box.appendChild(summary);

  var keywordBox = document.createElement("div");
  keywordBox.className = "keywords";
  for (var i = 0; i < paper.keywords.length; i++) {
    var tag = document.createElement("span");
    tag.textContent = paper.keywords[i];
    keywordBox.appendChild(tag);
  }
  box.appendChild(keywordBox);

  return box;
}

// Redraw the list according to the search box and the category drop-down.
function render() {
  var searchText = searchBox.value.toLowerCase();
  var chosenCategory = categoryBox.value;
  listDiv.innerHTML = "";
  var shown = 0;

  for (var i = 0; i < papers.length; i++) {
    if (paperMatches(papers[i], searchText, chosenCategory)) {
      listDiv.appendChild(createPaperElement(papers[i]));
      shown = shown + 1;
    }
  }
  countSpan.textContent = shown + " / " + papers.length + " papers";
}

searchBox.addEventListener("input", render);
categoryBox.addEventListener("change", render);
fillCategories();
render();
</script>
</body>
</html>
"""


def generate_html_page():
    """
    Rebuild output/index.html from all the papers in the database.

    The folder is created if needed. "</" is escaped in the JSON so that a
    paper text can never close the <script> tag by accident.
    """
    papers = get_all_papers()
    papers_json = json.dumps(papers, ensure_ascii=False)
    papers_json = papers_json.replace("</", "<\\/")

    html = HTML_TEMPLATE.replace("__PAPERS_JSON__", papers_json)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(HTML_PATH, "w", encoding="utf-8") as html_file:
        html_file.write(html)

    print("HTML page written to " + HTML_PATH + " (" + str(len(papers)) + " papers)")


# ============================================================================
# SECTION 7: Discord notification (step 8)
# ============================================================================
def send_discord_message(text):
    """
    Post a text message to the Discord channel linked to the webhook.

    If DISCORD_WEBHOOK_URL is empty, nothing is sent (a message is printed).
    Raises an error if Discord answers with an error code.
    """
    if DISCORD_WEBHOOK_URL == "":
        print("DISCORD_WEBHOOK_URL is not set, skipping notification.")
        return

    response = requests.post(DISCORD_WEBHOOK_URL, json={"content": text}, timeout=15)
    response.raise_for_status()


# ============================================================================
# SECTION 8: The Airflow DAG and its tasks (step 3)
# ============================================================================
# Runs every 5 minutes. catchup=False means Airflow does not replay missed
# runs. max_active_runs=1 prevents two runs processing the same PDF at once.
with DAG(
    dag_id="paper_pipeline",
    description="PDF -> Markdown -> LLM metadata -> SQLite -> HTML -> Discord",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["info-h420", "papers"],
) as dag:

    @task
    def find_new_pdfs():
        """
        TASK 1: list the PDFs in papers/ that are not yet in the database.

        Returns a list of file names (they are passed to the next task by Airflow).
        """
        already_done = get_processed_filenames()

        if not os.path.isdir(PAPERS_DIR):
            raise FileNotFoundError("Papers folder not found: " + PAPERS_DIR)

        new_files = []
        for name in sorted(os.listdir(PAPERS_DIR)):
            is_pdf = name.lower().endswith(".pdf")
            if is_pdf and name not in already_done:
                new_files.append(name)

        print("New PDFs found: " + str(new_files))
        return new_files

    @task
    def convert_pdfs(filenames):
        """
        TASK 2: convert each new PDF to a Markdown file in data/markdown/.

        The Markdown is saved on disk (not passed through Airflow) because it
        can be very large. A PDF that fails is skipped and will be tried again
        at the next run. Returns the list of file names converted successfully.
        """
        os.makedirs(MARKDOWN_DIR, exist_ok=True)

        converted = []
        for filename in filenames:
            pdf_path = os.path.join(PAPERS_DIR, filename)
            try:
                markdown_text = convert_pdf_to_markdown(pdf_path)
            except Exception as error:
                print("Could not convert " + filename + ": " + str(error))
                continue

            markdown_path = os.path.join(MARKDOWN_DIR, filename + ".md")
            with open(markdown_path, "w", encoding="utf-8") as markdown_file:
                markdown_file.write(markdown_text)

            converted.append(filename)
            print("Converted " + filename)

        return converted

    @task
    def extract_and_store(filenames):
        """
        TASK 3: for each converted paper, ask the LLM for the metadata and
        save it in SQLite.

        A paper whose LLM answer stays invalid after the retry is skipped
        and will be tried again at the next run.
        Returns a list of {"filename", "title"} for the papers saved.
        """
        stored = []
        for filename in filenames:
            markdown_path = os.path.join(MARKDOWN_DIR, filename + ".md")
            with open(markdown_path, "r", encoding="utf-8") as markdown_file:
                markdown_text = markdown_file.read()

            try:
                metadata = extract_metadata(markdown_text)
            except Exception as error:
                print("LLM extraction failed for " + filename + ": " + str(error))
                continue

            save_paper(filename, metadata)
            stored.append({"filename": filename, "title": metadata["title"]})
            print("Saved " + filename + " -> " + metadata["title"])

        return stored

    @task
    def generate_html(stored):
        """
        TASK 4: regenerate output/index.html from the database.

        'stored' is not used inside; it is a parameter only so that Airflow
        runs this task after extract_and_store.
        """
        generate_html_page()

    @task
    def notify_discord(stored):
        """
        TASK 5: send one Discord message per paper processed in this run.

        Nothing is sent when no new paper was processed.
        """
        for paper in stored:
            message = "New paper processed: **" + paper["title"] + "** (" + paper["filename"] + ")"
            send_discord_message(message)

    # ----- Task dependencies -------------------------------------------------
    # find_new_pdfs -> convert_pdfs -> extract_and_store -> generate_html -> notify_discord
    new_files = find_new_pdfs()
    converted_files = convert_pdfs(new_files)
    stored_papers = extract_and_store(converted_files)
    html_task = generate_html(stored_papers)
    notify_task = notify_discord(stored_papers)
    html_task >> notify_task
