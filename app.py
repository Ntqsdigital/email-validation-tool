import os
import pandas as pd
from datetime import datetime
from functools import wraps

import mysql.connector
from flask import (
    Flask, render_template, request,
    send_file, redirect, url_for, session
)
from email_validator import validate_email
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = "change_this_secret_key"

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER


# -------------------------
# MySQL connection
# -------------------------
def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Parlapalli@56",
        database="email_validator"
    )


# -------------------------
# Auth helper
# -------------------------
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            if request.method == "GET":
                session["next"] = request.path
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


# -------------------------
# Get user plan
# -------------------------
def get_user_plan():
    if "user_id" not in session:
        return "guest"

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT plan FROM users WHERE id=%s",
        (session["user_id"],)
    )
    row = cursor.fetchone()
    cursor.close()
    db.close()

    return row[0] if row else "free"


# -------------------------
# Count user validations
# -------------------------
def get_validation_count(user_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM history WHERE user_id=%s",
        (user_id,)
    )
    count = cursor.fetchone()[0]
    cursor.close()
    db.close()
    return count


# -------------------------
# Email validation
# -------------------------
def is_valid_email(email):
    try:
        validate_email(email, check_deliverability=False)
        return True
    except:
        return False


# -------------------------
# Signup
# -------------------------
@app.route("/signup", methods=["GET", "POST"])
def signup():
    error = None

    if request.method == "POST":
        email = request.form["email"].lower().strip()
        password = request.form["password"]

        db = get_db()
        cursor = db.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO users (email, password, plan, subscription_status)
                VALUES (%s, %s, 'free', 'inactive')
                """,
                (email, generate_password_hash(password))
            )
            db.commit()
            return redirect(url_for("login"))
        except mysql.connector.IntegrityError:
            error = "Account already exists"
        finally:
            cursor.close()
            db.close()

    return render_template("signup.html", error=error)


# -------------------------
# Login
# -------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None

    if request.method == "POST":
        email = request.form["email"].lower().strip()
        password = request.form["password"]

        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            "SELECT id, password FROM users WHERE email=%s",
            (email,)
        )
        user = cursor.fetchone()
        cursor.close()
        db.close()

        if user and check_password_hash(user[1], password):
            session["user_id"] = user[0]
            session["email"] = email
            return redirect(url_for("dashboard"))
        else:
            error = "Invalid email or password"

    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("dashboard"))


# -------------------------
# Public Dashboard
# -------------------------
@app.route("/")
def home():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    return render_template(
        "dashboard.html",
        page="dashboard",
        user_email=session.get("email"),
        plan=get_user_plan()
    )


# -------------------------
# History
# -------------------------
@app.route("/history")
@login_required
def history():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT filename, total, valid, invalid, created_at
        FROM history
        WHERE user_id=%s
        ORDER BY id DESC
        """,
        (session["user_id"],)
    )
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    return render_template(
        "dashboard.html",
        page="history",
        history=rows,
        user_email=session["email"],
        plan=get_user_plan()
    )


# -------------------------
# Billing
# -------------------------
@app.route("/billing")
@login_required
def billing():
    return render_template(
        "dashboard.html",
        page="billing",
        user_email=session["email"],
        plan=get_user_plan()
    )


# -------------------------
# Validate (FREE = 1 TIME)
# -------------------------
@app.route("/validate", methods=["POST"])
@login_required
def validate_file():
    user_plan = get_user_plan()
    validations_done = get_validation_count(session["user_id"])

    # 🚫 Free user restriction
    if user_plan == "free" and validations_done >= 1:
        return redirect(url_for("billing"))

    file = request.files.get("file")
    if not file:
        return redirect(url_for("dashboard"))

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(filepath)

    if filename.lower().endswith(".csv"):
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)

    df.columns = [c.strip().lower() for c in df.columns]
    email_column = next((c for c in df.columns if "email" in c), None)
    if not email_column:
        return redirect(url_for("dashboard"))

    valid_emails, invalid_emails = [], []

    for email in df[email_column]:
        if pd.isna(email):
            continue
        email = str(email).strip().lower()
        (valid_emails if is_valid_email(email) else invalid_emails).append(email)

    pd.DataFrame(valid_emails, columns=["email"]).to_csv(
        os.path.join(OUTPUT_FOLDER, "valid_emails.csv"), index=False
    )
    pd.DataFrame(invalid_emails, columns=["email"]).to_csv(
        os.path.join(OUTPUT_FOLDER, "invalid_emails.csv"), index=False
    )

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        INSERT INTO history
        (user_id, filename, total, valid, invalid, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            session["user_id"],
            filename,
            len(valid_emails) + len(invalid_emails),
            len(valid_emails),
            len(invalid_emails),
            datetime.now().strftime("%Y-%m-%d %H:%M")
        )
    )
    db.commit()
    cursor.close()
    db.close()

    return render_template(
        "dashboard.html",
        page="dashboard",
        user_email=session["email"],
        plan=get_user_plan(),
        valid_count=len(valid_emails),
        invalid_count=len(invalid_emails),
        show_results=True
    )


# -------------------------
# Downloads
# -------------------------
@app.route("/download/valid")
@login_required
def download_valid():
    return send_file("outputs/valid_emails.csv", as_attachment=True)


@app.route("/download/invalid")
@login_required
def download_invalid():
    return send_file("outputs/invalid_emails.csv", as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
