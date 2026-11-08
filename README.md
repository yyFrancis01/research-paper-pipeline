# Research Library Pipeline (INFO-H420, Assignment 3)

A small local pipeline, orchestrated by Apache Airflow, that turns research PDFs into a searchable study library.

```
papers/*.pdf --> Markdown --> local LLM (JSON) --> SQLite --> index.html --> Discord notification
```

The DAG `paper_pipeline` runs every 5 minutes and has 5 tasks, executed one after the other:

| # | Task | What it does |
|---|------|--------------|
| 1 | `find_new_pdfs` | Lists PDFs in `papers/` that are not yet in the database |
| 2 | `convert_pdfs` | Converts each new PDF to Markdown (PyMuPDF4LLM) |
| 3 | `extract_and_store` | Asks the local LLM for title, summary, keywords, category (JSON, one retry), saves to SQLite |
| 4 | `generate_html` | Regenerates `output/index.html` (search box + category filter) |
| 5 | `notify_discord` | Sends one Discord message per processed paper |

---

## 1. Requirements

| Requirement | Notes |
|---|---|
| Python 3.10<= | https://www.python.org/downloads/ |
| Apache Airflow | Installed with the official constraints file (see below) |
| `pymupdf` | PDF to Markdown |
| `openai` | Python client used to call the local LLM |
| `requests` | Discord webhook (already installed with Airflow) |
| `sqlite` | Database |
| Ollama | https://ollama.com/download, serves the local LLM |
| A Discord webhook | Optional for testing, required for the assignment |

---

## 2. Installation

### 2.1 Project folders

```bash
mkdir -p paper_project/dags/
cd paper_project/
```

Copy `paper_pipeline.py` into `~/paper_project/dags/` and `requirements.txt` into `~/paper_project/`.

Final layout:

```
Google/ResearchLibrary/            (local)
├── papers/                          <- put your PDFs here
└── output/                          <- index.html is generated here

paper_project/                     (local, NOT synced)
├── dags/paper_pipeline.py
├── requirements.txt
└── data/                            <- created automatically
    ├── papers.db
    └── markdown/
```

Create the two Google Drive folders if they do not exist:

```bash
mkdir ../Google/ResearchLibrary/papers
mkdir ../Google/ResearchLibrary/output
```

### 2.2 Python virtual environment and Airflow

If your distro doesn't include venv don't forget to launch this before anything:
```bash
sudo apt install python3-venv python3-pip
```
If this fails, follow the current official page: (https://airflow.apache.org/docs/apache-airflow/stable/start.html). Note: here we use the 3.0.6 version of Airflow because we use Python <= 3.10

Before launching this code make sure to be in the `~/paper_project/` folder 
```bash
python3 -m venv .venv
source .venv/bin/activate
#pip install --upgrade pip

AIRFLOW_VERSION=3.0.6
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
```





### 2.3 Local LLM (Ollama)

1. Install Ollama from https://ollama.com/download.
2. Download a model (about 2 GB):

```bash
ollama pull llama3.2
```

3. Check that it answers:

```bash
curl http://localhost:11434/v1/models
```

### 2.4 Discord webhook

1. In Discord, open your server: **Server Settings > Integrations > Webhooks > New Webhook**.
2. Choose a channel and click **Copy Webhook URL**.

---

## 3. Commands to run BEFORE launching Airflow

Every time you open a new terminal, run these commands first. The environment variables must be exported **in the same terminal** that starts `airflow standalone`.

```bash
cd paper_project/
source .venv/bin/activate

```

Then, specify your paths for Airflow (by default, we start at `$HOME`)
```bash
#Change this line if you want the project in a specific folder
PROJECT_FOLDER="$HOME"

#Folder synced by Google Drive (contains papers/ and output/)
export LIBRARY_DIR="$PROJECT_FOLDER/Google/ResearchLibrary"

#Local project folder (SQLite database lives here, outside Google Drive)
export PROJECT_DIR="$PROJECT_FOLDER/paper_project"

#Airflow configuration
export AIRFLOW_HOME="$PROJECT_FOLDER/paper_project/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_FOLDER/paper_project/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

#Local LLM (Ollama defaults, change only if needed)
export LLM_BASE_URL="http://localhost:11434/v1"
export LLM_MODEL="llama3.2"

#Discord webhook (leave unset to skip notifications while testing)
#export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/XXXX/YYYY"
```

---

## 4. Launching

Terminal 1 (skip it if the Ollama desktop app is already running):

```bash
ollama serve
```

Terminal 2 (after running the commands of section 3):

```bash
airflow standalone
```

Airflow prints the admin username and password in the terminal (look for a line like `Login with username: admin  password: ...`). The password is also saved in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` (Airflow 3) or `$AIRFLOW_HOME/standalone_admin_password.txt` (Airflow 2).

---

## 5. Example of execution (step by step)

1. **Add a PDF.** Copy a research paper into the synced folder (`ResearchLibrary/papers`):

   ```bash
   cp ~/Downloads/my_paper.pdf "$LIBRARY_DIR/papers/"
   ```

2. **Open Airflow.** Go to http://localhost:8080 and log in with admin and the password printed in the terminal.

3. **Enable the DAG.** In the DAG list, find `paper_pipeline` by refreshing (F5) and switch the toggle on (un-pause). It will then runs automatically

4. **Trigger it now.** Click `paper_pipeline`, then the **Trigger** button (play icon, top right) to avoid waiting.

5. **Watch it run.** In the **Graph** view the 5 tasks turn green one after the other:

   ```
   find_new_pdfs -> convert_pdfs -> extract_and_store -> generate_html -> notify_discord
   ```

   Click a task, then **Logs**, to see what it did. Example log of `extract_and_store`:

   ```
   Saved my_paper.pdf -> TITLE
   ```

6. **Check the results.**

   - Database:

     ```bash
     sqlite3 "$PROJECT_DIR/data/papers.db" "SELECT filename, title, category FROM papers;"
     ```

     Expected output (example):

     ```
     my_paper.pdf|TITLE|CATEGORY
     ```

   - Web page: open `$LIBRARY_DIR/output/index.html` in a browser. Type in the search box or pick a category to filter the papers.

   - Discord: a message such as `New paper processed: **TITLE** (my_paper.pdf)` appears in your channel.

7. **Test the "new paper only" behaviour.** Trigger the DAG again without adding anything: `find_new_pdfs` returns an empty list, nothing is reprocessed and no Discord message is sent. Add a second PDF and trigger again: only the new one is processed.

---

## 6. Configuration reference

All values are read from environment variables (defaults in `dags/paper_pipeline.py`, Section 2).

| Variable | Default | Meaning |
|---|---|---|
| `LIBRARY_DIR` | `~/Google Drive/ResearchLibrary` | Synced folder with `papers/` and `output/` |
| `PROJECT_DIR` | `~/paper_project` | Local folder holding `data/papers.db` |
| `LLM_BASE_URL` | `http://localhost:11434/v1` | OpenAI-compatible endpoint (Ollama; LM Studio uses `http://localhost:1234/v1`) |
| `LLM_API_KEY` | `ollama` | Ignored by local servers but required by the client |
| `LLM_MODEL` | `llama3.2` | Choosen model name as shown by `ollama list` |
| `DISCORD_WEBHOOK_URL` | empty | Webhook URL; if empty, notifications are skipped |

---

## 7. Troubleshooting

| Problem | Solution |
|---|---|
| `Papers folder not found` in the `find_new_pdfs` log | `LIBRARY_DIR` is wrong or was not exported in the terminal that started Airflow. Stop Airflow (Ctrl+C), export it, restart. |
| DAG does not appear in Airflow | Check `AIRFLOW__CORE__DAGS_FOLDER`. Look for import errors at the top of the DAG list (missing package, wrong virtual environment). |
| `Connection error` in `extract_and_store` | Ollama is not running (`ollama serve`) or the model is not downloaded (`ollama pull llama3.2`)|
| LLM errors about `response_format` | Your model or server does not support JSON mode. Remove the `response_format=...` line in `call_the_T_800` in `paper_pipeline.py`|
| A PDF is never stored | Its conversion or LLM answer failed. See the task logs. It is retried automatically at each run. |
| No Discord message | `DISCORD_WEBHOOK_URL` is not set or invalid. Check the `notify_discord` log. And make sure you didn't forgot the init the URL for discord|
| Model too slow | Use a smaller model (for example `llama3.2:1b`) and set `LLM_MODEL` accordingly. |
