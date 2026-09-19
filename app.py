# -*- coding: utf-8 -*-
"""
ระบบติดตามงาน (Task Tracking System)
------------------------------------
Single-file Flask application

แนวคิดหลัก:  งาน  ->  ขั้นตอนการดำเนินงาน  ->  เอกสาร/หนังสือที่แนบในแต่ละขั้นตอน
  * สร้างงาน กำหนดผู้รับผิดชอบ และกำหนดส่ง
  * แบ่งงานเป็นขั้นตอน แต่ละขั้นตอนมีผู้รับผิดชอบ/กำหนดส่ง/สถานะของตัวเอง
  * แนบหนังสือเข้า หนังสือออก หรือเอกสารประกอบ ได้ "หลายไฟล์ หลายครั้ง" ทุกขั้นตอน
  * รายงานความคืบหน้า แนบไฟล์ไปพร้อมกับรายงานได้
  * สรุปรายงานต่าง ๆ และส่งออก CSV

วิธีใช้:
    pip install flask
    python app.py
    เปิด http://127.0.0.1:5000
"""

import csv
import io
import os
import shutil
import sqlite3
import sys
import uuid
from datetime import date, datetime, timedelta

from flask import (Flask, Response, abort, flash, g, redirect, render_template,
                   request, send_from_directory, url_for)
from jinja2 import DictLoader

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "tracker.db")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
ALLOWED_EXT = {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
               "png", "jpg", "jpeg", "gif", "zip", "rar", "txt", "csv"}

STATUSES = ["รอดำเนินการ", "กำลังดำเนินการ", "รอตรวจสอบ", "เสร็จสิ้น", "ยกเลิก"]
PRIORITIES = ["ต่ำ", "ปกติ", "สูง", "ด่วนที่สุด"]
STEP_STATUSES = ["รอดำเนินการ", "กำลังดำเนินการ", "เสร็จสิ้น"]
CATEGORIES = ["งานบริหาร", "งานวิชาการ", "งานการเงิน", "งานพัสดุ",
              "งานบุคคล", "งานโครงการ", "อื่น ๆ"]
KINDS = ["หนังสือเข้า", "หนังสือออก", "เอกสารประกอบ"]

app = Flask(__name__)
app.config["SECRET_KEY"] = "task-tracker-secret-key-change-me"
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024  # 32 MB ต่อการอัปโหลด 1 ครั้ง
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    description TEXT,
    category    TEXT,
    owner       TEXT,
    assignee    TEXT,
    priority    TEXT NOT NULL DEFAULT 'ปกติ',
    status      TEXT NOT NULL DEFAULT 'รอดำเนินการ',
    progress    INTEGER NOT NULL DEFAULT 0,
    start_date  TEXT,
    due_date    TEXT,
    done_date   TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- ขั้นตอนการดำเนินงานของแต่ละงาน
CREATE TABLE IF NOT EXISTS steps (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    seq        INTEGER NOT NULL DEFAULT 1,
    name       TEXT NOT NULL,
    owner      TEXT,
    due_date   TEXT,
    status     TEXT NOT NULL DEFAULT 'รอดำเนินการ',
    done_date  TEXT,
    note       TEXT,
    created_at TEXT NOT NULL
);

-- รายงานความคืบหน้า (ผูกกับงาน และผูกกับขั้นตอนได้)
CREATE TABLE IF NOT EXISTS updates (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    step_id    INTEGER REFERENCES steps(id) ON DELETE SET NULL,
    note       TEXT NOT NULL,
    progress   INTEGER,
    status     TEXT,
    reporter   TEXT,
    created_at TEXT NOT NULL
);

-- เอกสาร/หนังสือที่แนบกับงาน หรือแนบกับขั้นตอน (แนบได้หลายไฟล์ หลายครั้ง)
CREATE TABLE IF NOT EXISTS files (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    step_id      INTEGER REFERENCES steps(id) ON DELETE SET NULL,
    update_id    INTEGER REFERENCES updates(id) ON DELETE SET NULL,
    kind         TEXT NOT NULL DEFAULT 'เอกสารประกอบ',  -- หนังสือเข้า / หนังสือออก / เอกสารประกอบ
    doc_no       TEXT,          -- เลขที่หนังสือ (ถ้ามี)
    doc_date     TEXT,          -- ลงวันที่ / วันที่ของเอกสาร
    subject      TEXT,          -- เรื่อง
    counterparty TEXT,          -- จาก / ถึง หน่วยงาน
    note         TEXT,
    uploader     TEXT,
    filename     TEXT,          -- ชื่อไฟล์เดิม
    stored_name  TEXT,          -- ชื่อไฟล์ที่เก็บจริง
    filesize     INTEGER,
    created_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_steps_task ON steps(task_id, seq);
CREATE INDEX IF NOT EXISTS idx_files_task ON files(task_id);
CREATE INDEX IF NOT EXISTS idx_files_step ON files(step_id);
"""


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def query(sql, args=(), one=False):
    cur = get_db().execute(sql, args)
    rows = cur.fetchall()
    cur.close()
    return (rows[0] if rows else None) if one else rows


def execute(sql, args=()):
    db = get_db()
    cur = db.execute(sql, args)
    db.commit()
    last = cur.lastrowid
    cur.close()
    return last


def init_db():
    """สร้างฐานข้อมูล และย้ายไฟล์ฐานข้อมูลรุ่นเก่า (โครงสร้างทะเบียนหนังสือ) ออกไปเก็บไว้"""
    if os.path.exists(DB_PATH):
        con = sqlite3.connect(DB_PATH)
        names = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        con.close()
        if "documents" in names and "files" not in names:
            backup = os.path.join(
                BASE_DIR, "tracker_old_%s.db" % datetime.now().strftime("%Y%m%d%H%M%S"))
            shutil.move(DB_PATH, backup)
            cprint(" * พบฐานข้อมูลโครงสร้างเดิม ย้ายไปเก็บไว้ที่ %s" % os.path.basename(backup))

    fresh = not os.path.exists(DB_PATH)
    con = sqlite3.connect(DB_PATH)
    con.executescript(SCHEMA)
    con.commit()
    con.close()
    if fresh:
        seed_demo()


def seed_demo():
    """ข้อมูลตัวอย่างเมื่อสร้างฐานข้อมูลครั้งแรก"""
    con = sqlite3.connect(DB_PATH)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = date.today()

    def d(n):
        return (today + timedelta(days=n)).isoformat()

    tasks = [
        # title, desc, category, owner, assignee, priority, status, start, due, done
        ("จัดทำแผนปฏิบัติการประจำปี 2569", "รวบรวมโครงการจากทุกกลุ่มงานและจัดทำเล่มแผนเสนอผู้บริหาร",
         "งานบริหาร", "ผอ.สมชาย", "นางสาวมาลี", "สูง", "กำลังดำเนินการ", d(-20), d(7), None),
        ("รายงานผลการเบิกจ่ายงบประมาณ ไตรมาส 4", "สรุปยอดเบิกจ่ายและวิเคราะห์ผลต่างเสนอกรม",
         "งานการเงิน", "หัวหน้าการเงิน", "นายวิชัย", "ด่วนที่สุด", "กำลังดำเนินการ", d(-8), d(2), None),
        ("จัดซื้อครุภัณฑ์คอมพิวเตอร์ 20 เครื่อง", "ดำเนินการจัดซื้อตามระเบียบพัสดุ",
         "งานพัสดุ", "หัวหน้าพัสดุ", "นายสมศักดิ์", "ปกติ", "กำลังดำเนินการ", d(-25), d(-2), None),
        ("อบรมการใช้งานระบบสารสนเทศภายใน", "จัดอบรมบุคลากร 2 รุ่น รุ่นละ 40 คน",
         "งานบุคคล", "หัวหน้าบุคคล", "นางสาวปนัดดา", "ปกติ", "เสร็จสิ้น", d(-40), d(-10), d(-11)),
        ("ติดตามโครงการพัฒนาชุมชนต้นแบบ", "ลงพื้นที่ติดตามผลการดำเนินงาน 5 ตำบล",
         "งานโครงการ", "ผอ.สมชาย", "นายวิชัย", "สูง", "กำลังดำเนินการ", d(-5), d(-3), None),
        ("ปรับปรุงฐานข้อมูลบุคลากร", "ตรวจสอบและปรับปรุงข้อมูลให้เป็นปัจจุบัน",
         "งานบุคคล", "หัวหน้าบุคคล", "นายธนา", "ต่ำ", "รอดำเนินการ", d(-2), d(25), None),
    ]
    for t in tasks:
        con.execute(
            """INSERT INTO tasks (title, description, category, owner, assignee, priority,
                   status, progress, start_date, due_date, done_date, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,0,?,?,?,?,?)""", t + (now, now))

    # (task_id, seq, ชื่อขั้นตอน, ผู้รับผิดชอบ, กำหนดส่ง, สถานะ)
    steps = [
        (1, 1, "รับหนังสือสั่งการจากกรม", "นางสาวมาลี", d(-19), "เสร็จสิ้น"),
        (1, 2, "แจ้งเวียนกลุ่มงานส่งข้อมูลโครงการ", "นางสาวมาลี", d(-12), "เสร็จสิ้น"),
        (1, 3, "รวบรวมและตรวจสอบข้อมูล", "นางสาวมาลี", d(-3), "เสร็จสิ้น"),
        (1, 4, "จัดรูปเล่มแผนปฏิบัติการ", "นางสาวมาลี", d(3), "กำลังดำเนินการ"),
        (1, 5, "เสนอผู้บริหารลงนามและส่งกรม", "ผอ.สมชาย", d(7), "รอดำเนินการ"),

        (2, 1, "ดึงข้อมูลการเบิกจ่ายจากระบบ", "นายวิชัย", d(-6), "เสร็จสิ้น"),
        (2, 2, "วิเคราะห์ผลต่างและจัดทำรายงาน", "นายวิชัย", d(0), "กำลังดำเนินการ"),
        (2, 3, "เสนอผู้บริหารลงนาม", "หัวหน้าการเงิน", d(1), "รอดำเนินการ"),
        (2, 4, "จัดส่งรายงานให้กรม", "นายวิชัย", d(2), "รอดำเนินการ"),

        (3, 1, "จัดทำร่างขอบเขตงาน (TOR)", "นายสมศักดิ์", d(-20), "เสร็จสิ้น"),
        (3, 2, "ขออนุมัติจัดซื้อ", "หัวหน้าพัสดุ", d(-14), "เสร็จสิ้น"),
        (3, 3, "ประกาศและรับข้อเสนอ", "นายสมศักดิ์", d(-6), "เสร็จสิ้น"),
        (3, 4, "ตรวจรับพัสดุ", "นายสมศักดิ์", d(-2), "กำลังดำเนินการ"),
        (3, 5, "เบิกจ่ายเงินให้ผู้ขาย", "หัวหน้าการเงิน", d(10), "รอดำเนินการ"),

        (4, 1, "ขออนุมัติโครงการอบรม", "นางสาวปนัดดา", d(-35), "เสร็จสิ้น"),
        (4, 2, "จัดอบรมรุ่นที่ 1-2", "นางสาวปนัดดา", d(-15), "เสร็จสิ้น"),
        (4, 3, "สรุปผลและรายงานผู้บริหาร", "นางสาวปนัดดา", d(-10), "เสร็จสิ้น"),

        (5, 1, "จัดทำแผนลงพื้นที่", "นายวิชัย", d(-4), "เสร็จสิ้น"),
        (5, 2, "ลงพื้นที่ตำบลที่ 1-3", "นายวิชัย", d(-3), "กำลังดำเนินการ"),
        (5, 3, "สรุปผลการติดตามเสนอผู้บริหาร", "นายวิชัย", d(5), "รอดำเนินการ"),

        (6, 1, "ออกแบบแบบฟอร์มเก็บข้อมูล", "นายธนา", d(5), "รอดำเนินการ"),
        (6, 2, "เก็บข้อมูลจากทุกกลุ่มงาน", "นายธนา", d(15), "รอดำเนินการ"),
        (6, 3, "บันทึกเข้าระบบและตรวจสอบ", "นายธนา", d(25), "รอดำเนินการ"),
    ]
    for s in steps:
        done = s[4] if s[5] == "เสร็จสิ้น" else None
        con.execute(
            """INSERT INTO steps (task_id, seq, name, owner, due_date, status, done_date, created_at)
               VALUES (?,?,?,?,?,?,?,?)""", s + (done, now))

    # (task_id, step_id, kind, doc_no, doc_date, subject, counterparty)
    docs = [
        (1, 1, "หนังสือเข้า", "นร 0505/ว89", d(-20), "ซักซ้อมแนวทางการจัดทำแผนปฏิบัติการประจำปี",
         "สำนักงานปลัดสำนักนายกรัฐมนตรี"),
        (1, 2, "หนังสือออก", "มท 5501/301", d(-12), "ขอความอนุเคราะห์ข้อมูลโครงการประจำปี 2569",
         "ทุกกลุ่มงานในสังกัด"),
        (1, 3, "เอกสารประกอบ", "", d(-4), "แบบสรุปโครงการที่ได้รับจากกลุ่มงาน (8 กลุ่ม)", ""),
        (2, 1, "หนังสือเข้า", "มท 0808.2/ว1234", d(-10), "ขอให้รายงานผลการเบิกจ่ายงบประมาณ ไตรมาส 4",
         "กรมส่งเสริมการปกครองท้องถิ่น"),
        (2, 2, "เอกสารประกอบ", "", d(-5), "ตารางวิเคราะห์ผลต่างการเบิกจ่าย", ""),
        (3, 2, "หนังสือออก", "มท 5501/276", d(-15), "ขออนุมัติจัดซื้อครุภัณฑ์คอมพิวเตอร์",
         "ผู้ว่าราชการจังหวัด"),
        (3, 3, "เอกสารประกอบ", "", d(-7), "ใบเสนอราคาจากผู้ประกอบการ 3 ราย", ""),
        (3, 4, "เอกสารประกอบ", "", d(-2), "บันทึกการตรวจรับพัสดุ (ฉบับร่าง)", ""),
        (4, 3, "หนังสือออก", "มท 5501/255", d(-10), "รายงานผลการจัดอบรมระบบสารสนเทศภายใน",
         "ผู้ว่าราชการจังหวัด"),
        (5, 1, "หนังสือเข้า", "มท 0810.3/ว77", d(-6), "แนวทางการติดตามโครงการพัฒนาชุมชนต้นแบบ",
         "กรมการพัฒนาชุมชน"),
    ]
    for f in docs:
        con.execute(
            """INSERT INTO files (task_id, step_id, kind, doc_no, doc_date, subject,
                   counterparty, note, uploader, created_at)
               VALUES (?,?,?,?,?,?,?,'บันทึกข้อมูลไว้ก่อน ยังไม่ได้แนบไฟล์สแกน','ระบบ',?)""",
            f + (now,))

    ups = [
        (1, 4, "รวบรวมข้อมูลครบ 8 กลุ่มงาน อยู่ระหว่างจัดรูปเล่ม คาดเสร็จภายในสัปดาห์นี้", "นางสาวมาลี"),
        (2, 2, "ดึงข้อมูลจากระบบเรียบร้อย พบผลต่าง 3 หมวด อยู่ระหว่างตรวจสอบกับหน่วยเบิกจ่าย", "นายวิชัย"),
        (3, 4, "คณะกรรมการตรวจรับครบถ้วนแล้ว รอผู้ขายแก้ไขเอกสารใบส่งของ", "นายสมศักดิ์"),
        (5, 2, "ลงพื้นที่แล้ว 1 ตำบล ติดฝนตกหนักต้องเลื่อนอีก 2 ตำบล", "นายวิชัย"),
    ]
    for u in ups:
        con.execute(
            """INSERT INTO updates (task_id, step_id, note, progress, status, reporter, created_at)
               VALUES (?,?,?,NULL,NULL,?,?)""", u + (now,))
    con.commit()

    # คำนวณความคืบหน้าของงานจากขั้นตอนที่เสร็จ
    for (tid,) in con.execute("SELECT id FROM tasks").fetchall():
        row = con.execute(
            """SELECT COUNT(*) total, SUM(CASE WHEN status='เสร็จสิ้น' THEN 1 ELSE 0 END) done
               FROM steps WHERE task_id=?""", (tid,)).fetchone()
        if row and row[0]:
            con.execute("UPDATE tasks SET progress=? WHERE id=?",
                        (round(row[1] * 100.0 / row[0]), tid))
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def today_str():
    return date.today().isoformat()


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


THAI_MONTHS = ["", "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.",
               "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
THAI_MONTHS_FULL = ["", "มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม",
                    "มิถุนายน", "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม",
                    "พฤศจิกายน", "ธันวาคม"]


@app.template_filter("thdate")
def thdate(value):
    if not value:
        return "-"
    try:
        dt = datetime.strptime(str(value)[:10], "%Y-%m-%d")
    except ValueError:
        return value
    return "%d %s %s" % (dt.day, THAI_MONTHS[dt.month], str(dt.year + 543)[-2:])


@app.template_filter("thdatetime")
def thdatetime(value):
    if not value:
        return "-"
    try:
        dt = datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value
    return "%d %s %s %02d:%02d" % (dt.day, THAI_MONTHS[dt.month],
                                   str(dt.year + 543)[-2:], dt.hour, dt.minute)


@app.template_filter("filesize")
def filesize_fmt(n):
    if not n:
        return ""
    n = float(n)
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return "%.0f %s" % (n, unit) if unit == "B" else "%.1f %s" % (n, unit)
        n /= 1024
    return "%.1f TB" % n


def days_left(due, status):
    """วันคงเหลือ (ลบ = เกินกำหนด); None เมื่อไม่มีกำหนดหรือปิดงานแล้ว"""
    if not due or status in ("เสร็จสิ้น", "ยกเลิก"):
        return None
    try:
        return (datetime.strptime(due, "%Y-%m-%d").date() - date.today()).days
    except ValueError:
        return None


app.jinja_env.globals.update(
    days_left=days_left, STATUSES=STATUSES, PRIORITIES=PRIORITIES,
    STEP_STATUSES=STEP_STATUSES, CATEGORIES=CATEGORIES, KINDS=KINDS,
    today_str=today_str,
)


def allowed_file(name):
    return "." in name and name.rsplit(".", 1)[1].lower() in ALLOWED_EXT


def save_attachments(task_id, step_id=None, update_id=None, form=None, field="files"):
    """บันทึกไฟล์แนบได้หลายไฟล์ต่อหนึ่งครั้ง และแนบซ้ำได้ไม่จำกัดจำนวนครั้ง

    คืนค่า (จำนวนไฟล์ที่บันทึก, [ชื่อไฟล์ที่ไม่รองรับ])
    """
    form = form if form is not None else {}
    uploads = [f for f in request.files.getlist(field) if f and f.filename]
    kind = form.get("kind") or "เอกสารประกอบ"
    doc_no = (form.get("doc_no") or "").strip()
    doc_date = form.get("doc_date") or None
    subject = (form.get("subject") or "").strip()
    counterparty = (form.get("counterparty") or "").strip()
    note = (form.get("note") or "").strip()
    uploader = (form.get("uploader") or "").strip()

    # บันทึกข้อมูลหนังสือได้แม้ยังไม่มีไฟล์แนบ (เช่น ลงเลขที่หนังสือไว้ก่อน)
    if not uploads:
        if subject or doc_no:
            execute(
                """INSERT INTO files (task_id, step_id, update_id, kind, doc_no, doc_date,
                       subject, counterparty, note, uploader, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (task_id, step_id, update_id, kind, doc_no, doc_date, subject,
                 counterparty, note, uploader, now_str()))
            return 1, []
        return 0, []

    saved, rejected = 0, []
    for up in uploads:
        if not allowed_file(up.filename):
            rejected.append(up.filename)
            continue
        ext = up.filename.rsplit(".", 1)[1].lower()
        stored = "%s.%s" % (uuid.uuid4().hex, ext)
        up.save(os.path.join(UPLOAD_DIR, stored))
        size = os.path.getsize(os.path.join(UPLOAD_DIR, stored))
        execute(
            """INSERT INTO files (task_id, step_id, update_id, kind, doc_no, doc_date,
                   subject, counterparty, note, uploader, filename, stored_name,
                   filesize, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (task_id, step_id, update_id, kind, doc_no, doc_date,
             subject or up.filename, counterparty, note, uploader,
             up.filename, stored, size, now_str()))
        saved += 1
    return saved, rejected


def flash_upload_result(saved, rejected):
    if saved:
        flash("แนบเอกสารเรียบร้อย %d รายการ" % saved, "success")
    if rejected:
        flash("ไม่รองรับไฟล์: %s" % ", ".join(rejected), "error")
    if not saved and not rejected:
        flash("ยังไม่ได้เลือกไฟล์หรือกรอกข้อมูลเอกสาร", "error")


def recompute_progress(task_id):
    """คำนวณ % ความคืบหน้าของงานจากขั้นตอนที่ทำเสร็จ (ถ้างานนั้นมีขั้นตอน)"""
    row = query("""SELECT COUNT(*) total,
                          SUM(CASE WHEN status='เสร็จสิ้น' THEN 1 ELSE 0 END) done
                   FROM steps WHERE task_id=?""", (task_id,), one=True)
    if not row or not row["total"]:
        return
    pct = int(round(row["done"] * 100.0 / row["total"]))
    task = query("SELECT status, done_date FROM tasks WHERE id=?", (task_id,), one=True)
    status, done_date = task["status"], task["done_date"]
    if 0 < pct < 100 and status == "รอดำเนินการ":
        status = "กำลังดำเนินการ"
    if pct == 100 and status in ("รอดำเนินการ", "กำลังดำเนินการ"):
        status = "รอตรวจสอบ"
    execute("UPDATE tasks SET progress=?, status=?, done_date=?, updated_at=? WHERE id=?",
            (pct, status, done_date, now_str(), task_id))


def get_task_or_404(task_id):
    t = query("SELECT * FROM tasks WHERE id=?", (task_id,), one=True)
    if not t:
        abort(404)
    return t


# ---------------------------------------------------------------------------
# Routes : Dashboard
# ---------------------------------------------------------------------------
@app.route("/")
def dashboard():
    total = query("SELECT COUNT(*) c FROM tasks", one=True)["c"]
    doing = query("SELECT COUNT(*) c FROM tasks WHERE status='กำลังดำเนินการ'", one=True)["c"]
    done = query("SELECT COUNT(*) c FROM tasks WHERE status='เสร็จสิ้น'", one=True)["c"]
    n_files = query("SELECT COUNT(*) c FROM files", one=True)["c"]

    overdue = query(
        """SELECT * FROM tasks
           WHERE due_date IS NOT NULL AND due_date <> '' AND due_date < ?
             AND status NOT IN ('เสร็จสิ้น','ยกเลิก') ORDER BY due_date""", (today_str(),))
    soon = query(
        """SELECT * FROM tasks
           WHERE due_date >= ? AND due_date <= ? AND status NOT IN ('เสร็จสิ้น','ยกเลิก')
           ORDER BY due_date""",
        (today_str(), (date.today() + timedelta(days=7)).isoformat()))
    steps_due = query(
        """SELECT s.*, t.title task_title FROM steps s JOIN tasks t ON t.id = s.task_id
           WHERE s.status <> 'เสร็จสิ้น' AND s.due_date IS NOT NULL AND s.due_date <> ''
             AND s.due_date <= ? AND t.status NOT IN ('เสร็จสิ้น','ยกเลิก')
           ORDER BY s.due_date LIMIT 8""",
        ((date.today() + timedelta(days=7)).isoformat(),))

    by_status = {s: 0 for s in STATUSES}
    for r in query("SELECT status, COUNT(*) c FROM tasks GROUP BY status"):
        by_status[r["status"]] = r["c"]
    by_priority = {p: 0 for p in PRIORITIES}
    for r in query("SELECT priority, COUNT(*) c FROM tasks GROUP BY priority"):
        by_priority[r["priority"]] = r["c"]

    recent_files = query(
        """SELECT f.*, t.title task_title, s.name step_name
           FROM files f JOIN tasks t ON t.id = f.task_id
           LEFT JOIN steps s ON s.id = f.step_id ORDER BY f.id DESC LIMIT 6""")
    recent_updates = query(
        """SELECT u.*, t.title, s.name step_name FROM updates u
           JOIN tasks t ON t.id = u.task_id LEFT JOIN steps s ON s.id = u.step_id
           ORDER BY u.id DESC LIMIT 6""")

    return render_template("dashboard.html", total=total, doing=doing, done=done,
                           n_files=n_files, overdue=overdue, soon=soon,
                           steps_due=steps_due, by_status=by_status,
                           by_priority=by_priority, recent_files=recent_files,
                           recent_updates=recent_updates)


# ---------------------------------------------------------------------------
# Routes : Tasks
# ---------------------------------------------------------------------------
@app.route("/tasks")
def task_list():
    q = request.args.get("q", "").strip()
    status = request.args.get("status", "")
    priority = request.args.get("priority", "")
    assignee = request.args.get("assignee", "")
    view = request.args.get("view", "")

    sql = """SELECT t.*,
                    (SELECT COUNT(*) FROM steps s WHERE s.task_id=t.id) n_steps,
                    (SELECT COUNT(*) FROM steps s WHERE s.task_id=t.id AND s.status='เสร็จสิ้น') n_done,
                    (SELECT COUNT(*) FROM files f WHERE f.task_id=t.id) n_files
             FROM tasks t WHERE 1=1"""
    args = []
    if q:
        sql += " AND (t.title LIKE ? OR t.description LIKE ? OR t.assignee LIKE ?)"
        args += ["%%%s%%" % q] * 3
    if status:
        sql += " AND t.status = ?"
        args.append(status)
    if priority:
        sql += " AND t.priority = ?"
        args.append(priority)
    if assignee:
        sql += " AND t.assignee = ?"
        args.append(assignee)
    if view == "overdue":
        sql += (" AND t.due_date IS NOT NULL AND t.due_date <> '' AND t.due_date < ?"
                " AND t.status NOT IN ('เสร็จสิ้น','ยกเลิก')")
        args.append(today_str())
    elif view == "open":
        sql += " AND t.status NOT IN ('เสร็จสิ้น','ยกเลิก')"
    sql += (" ORDER BY CASE WHEN t.status IN ('เสร็จสิ้น','ยกเลิก') THEN 1 ELSE 0 END,"
            " CASE WHEN t.due_date IS NULL OR t.due_date='' THEN 1 ELSE 0 END, t.due_date")

    tasks = query(sql, args)
    assignees = [r["assignee"] for r in query(
        "SELECT DISTINCT assignee FROM tasks WHERE assignee <> '' ORDER BY assignee")]
    return render_template("tasks.html", tasks=tasks, q=q, status=status,
                           priority=priority, assignee=assignee, view=view,
                           assignees=assignees)


@app.route("/tasks/new", methods=["GET", "POST"])
def task_new():
    if request.method == "POST":
        f = request.form
        if not f.get("title", "").strip():
            flash("กรุณากรอกชื่องาน", "error")
            return render_template("task_form.html", task=f, mode="new", steps_text="")
        tid = execute(
            """INSERT INTO tasks (title, description, category, owner, assignee, priority,
                   status, progress, start_date, due_date, done_date, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (f["title"].strip(), f.get("description", "").strip(), f.get("category", ""),
             f.get("owner", "").strip(), f.get("assignee", "").strip(),
             f.get("priority", "ปกติ"), f.get("status", "รอดำเนินการ"),
             int(f.get("progress") or 0), f.get("start_date") or None,
             f.get("due_date") or None, None, now_str(), now_str()))

        # ขั้นตอนเริ่มต้น: กรอกบรรทัดละ 1 ขั้นตอน
        seq = 0
        for line in (f.get("steps_text") or "").splitlines():
            line = line.strip()
            if line:
                seq += 1
                execute("""INSERT INTO steps (task_id, seq, name, owner, due_date, status, created_at)
                           VALUES (?,?,?,?,?,'รอดำเนินการ',?)""",
                        (tid, seq, line, f.get("assignee", "").strip(), None, now_str()))
        flash("สร้างงานเรียบร้อยแล้ว" + (" พร้อมขั้นตอน %d รายการ" % seq if seq else ""), "success")
        return redirect(url_for("task_detail", task_id=tid))
    return render_template("task_form.html", task=None, mode="new", steps_text="")


@app.route("/tasks/<int:task_id>")
def task_detail(task_id):
    task = get_task_or_404(task_id)
    steps = query("SELECT * FROM steps WHERE task_id=? ORDER BY seq, id", (task_id,))
    files = query("SELECT * FROM files WHERE task_id=? ORDER BY id DESC", (task_id,))
    updates = query(
        """SELECT u.*, s.name step_name FROM updates u
           LEFT JOIN steps s ON s.id = u.step_id
           WHERE u.task_id=? ORDER BY u.id DESC""", (task_id,))
    files_by_step = {}
    for f in files:
        files_by_step.setdefault(f["step_id"], []).append(f)
    return render_template("task_detail.html", task=task, steps=steps, files=files,
                           updates=updates, files_by_step=files_by_step)


@app.route("/tasks/<int:task_id>/edit", methods=["GET", "POST"])
def task_edit(task_id):
    task = get_task_or_404(task_id)
    if request.method == "POST":
        f = request.form
        status = f.get("status", "รอดำเนินการ")
        done_date = task["done_date"]
        if status == "เสร็จสิ้น":
            done_date = done_date or today_str()
        else:
            done_date = None
        execute(
            """UPDATE tasks SET title=?, description=?, category=?, owner=?, assignee=?,
                   priority=?, status=?, progress=?, start_date=?, due_date=?, done_date=?,
                   updated_at=? WHERE id=?""",
            (f["title"].strip(), f.get("description", "").strip(), f.get("category", ""),
             f.get("owner", "").strip(), f.get("assignee", "").strip(),
             f.get("priority", "ปกติ"), status, int(f.get("progress") or 0),
             f.get("start_date") or None, f.get("due_date") or None, done_date,
             now_str(), task_id))
        flash("บันทึกการแก้ไขเรียบร้อยแล้ว", "success")
        return redirect(url_for("task_detail", task_id=task_id))
    return render_template("task_form.html", task=task, mode="edit", steps_text="")


@app.route("/tasks/<int:task_id>/delete", methods=["POST"])
def task_delete(task_id):
    get_task_or_404(task_id)
    for f in query("SELECT stored_name FROM files WHERE task_id=?", (task_id,)):
        if f["stored_name"]:
            try:
                os.remove(os.path.join(UPLOAD_DIR, f["stored_name"]))
            except OSError:
                pass
    execute("DELETE FROM files WHERE task_id=?", (task_id,))
    execute("DELETE FROM updates WHERE task_id=?", (task_id,))
    execute("DELETE FROM steps WHERE task_id=?", (task_id,))
    execute("DELETE FROM tasks WHERE id=?", (task_id,))
    flash("ลบงานพร้อมขั้นตอนและเอกสารแนบเรียบร้อยแล้ว", "success")
    return redirect(url_for("task_list"))


@app.route("/tasks/<int:task_id>/report", methods=["POST"])
def task_report(task_id):
    task = get_task_or_404(task_id)
    f = request.form
    note = f.get("note", "").strip()
    if not note:
        flash("กรุณากรอกรายละเอียดการรายงาน", "error")
        return redirect(url_for("task_detail", task_id=task_id))
    step_id = f.get("step_id") or None
    uid = execute(
        """INSERT INTO updates (task_id, step_id, note, progress, status, reporter, created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (task_id, step_id, note, int(f.get("progress") or task["progress"]),
         f.get("status") or task["status"], f.get("reporter", "").strip(), now_str()))

    # แนบไฟล์ไปพร้อมกับรายงานได้ (หลายไฟล์)
    saved, rejected = save_attachments(task_id, step_id, uid, form=f)
    if rejected:
        flash("ไม่รองรับไฟล์: %s" % ", ".join(rejected), "error")

    status = f.get("status") or task["status"]
    done_date = task["done_date"]
    progress = int(f.get("progress") or task["progress"])
    if status == "เสร็จสิ้น":
        done_date = done_date or today_str()
        progress = 100
    else:
        done_date = None
    execute("UPDATE tasks SET progress=?, status=?, done_date=?, updated_at=? WHERE id=?",
            (progress, status, done_date, now_str(), task_id))
    flash("บันทึกรายงานความคืบหน้าแล้ว" + (" (แนบไฟล์ %d รายการ)" % saved if saved else ""),
          "success")
    return redirect(url_for("task_detail", task_id=task_id) + "#updates")


# ---------------------------------------------------------------------------
# Routes : Steps (ขั้นตอนการดำเนินงาน)
# ---------------------------------------------------------------------------
@app.route("/tasks/<int:task_id>/steps", methods=["POST"])
def step_add(task_id):
    get_task_or_404(task_id)
    f = request.form
    name = f.get("name", "").strip()
    if not name:
        flash("กรุณากรอกชื่อขั้นตอน", "error")
        return redirect(url_for("task_detail", task_id=task_id))
    row = query("SELECT COALESCE(MAX(seq),0) m FROM steps WHERE task_id=?", (task_id,), one=True)
    execute("""INSERT INTO steps (task_id, seq, name, owner, due_date, status, note, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (task_id, row["m"] + 1, name, f.get("owner", "").strip(),
             f.get("due_date") or None, f.get("status", "รอดำเนินการ"),
             f.get("note", "").strip(), now_str()))
    recompute_progress(task_id)
    flash("เพิ่มขั้นตอน “%s” แล้ว" % name, "success")
    return redirect(url_for("task_detail", task_id=task_id) + "#steps")


@app.route("/steps/<int:step_id>/update", methods=["POST"])
def step_update(step_id):
    step = query("SELECT * FROM steps WHERE id=?", (step_id,), one=True)
    if not step:
        abort(404)
    f = request.form
    status = f.get("status", step["status"])
    done_date = today_str() if status == "เสร็จสิ้น" else None
    execute("""UPDATE steps SET name=?, owner=?, due_date=?, status=?, done_date=?, note=?
               WHERE id=?""",
            (f.get("name", step["name"]).strip(), f.get("owner", "").strip(),
             f.get("due_date") or None, status, done_date,
             f.get("note", "").strip(), step_id))
    recompute_progress(step["task_id"])
    flash("ปรับปรุงขั้นตอนเรียบร้อยแล้ว", "success")
    return redirect(url_for("task_detail", task_id=step["task_id"]) + "#steps")


@app.route("/steps/<int:step_id>/status/<status>", methods=["POST"])
def step_status(step_id, status):
    step = query("SELECT * FROM steps WHERE id=?", (step_id,), one=True)
    if not step or status not in STEP_STATUSES:
        abort(404)
    execute("UPDATE steps SET status=?, done_date=? WHERE id=?",
            (status, today_str() if status == "เสร็จสิ้น" else None, step_id))
    recompute_progress(step["task_id"])
    return redirect(url_for("task_detail", task_id=step["task_id"]) + "#steps")


@app.route("/steps/<int:step_id>/move/<direction>", methods=["POST"])
def step_move(step_id, direction):
    step = query("SELECT * FROM steps WHERE id=?", (step_id,), one=True)
    if not step:
        abort(404)
    op, order = ("<", "DESC") if direction == "up" else (">", "ASC")
    other = query("SELECT * FROM steps WHERE task_id=? AND seq %s ? ORDER BY seq %s LIMIT 1"
                  % (op, order), (step["task_id"], step["seq"]), one=True)
    if other:
        execute("UPDATE steps SET seq=? WHERE id=?", (other["seq"], step["id"]))
        execute("UPDATE steps SET seq=? WHERE id=?", (step["seq"], other["id"]))
    return redirect(url_for("task_detail", task_id=step["task_id"]) + "#steps")


@app.route("/steps/<int:step_id>/delete", methods=["POST"])
def step_delete(step_id):
    step = query("SELECT * FROM steps WHERE id=?", (step_id,), one=True)
    if not step:
        abort(404)
    execute("UPDATE files SET step_id=NULL WHERE step_id=?", (step_id,))
    execute("UPDATE updates SET step_id=NULL WHERE step_id=?", (step_id,))
    execute("DELETE FROM steps WHERE id=?", (step_id,))
    recompute_progress(step["task_id"])
    flash("ลบขั้นตอนแล้ว (เอกสารที่แนบไว้ย้ายไปอยู่ระดับงาน)", "success")
    return redirect(url_for("task_detail", task_id=step["task_id"]) + "#steps")


# ---------------------------------------------------------------------------
# Routes : Files (เอกสาร/หนังสือแนบ)
# ---------------------------------------------------------------------------
@app.route("/tasks/<int:task_id>/upload", methods=["POST"])
def file_upload(task_id):
    get_task_or_404(task_id)
    step_id = request.form.get("step_id") or None
    saved, rejected = save_attachments(task_id, step_id, None, form=request.form)
    flash_upload_result(saved, rejected)
    anchor = "#step-%s" % step_id if step_id else "#files"
    return redirect(url_for("task_detail", task_id=task_id) + anchor)


@app.route("/files/<int:file_id>/download")
def file_download(file_id):
    f = query("SELECT * FROM files WHERE id=?", (file_id,), one=True)
    if not f or not f["stored_name"]:
        abort(404)
    return send_from_directory(UPLOAD_DIR, f["stored_name"], as_attachment=True,
                               download_name=f["filename"])


@app.route("/files/<int:file_id>/delete", methods=["POST"])
def file_delete(file_id):
    f = query("SELECT * FROM files WHERE id=?", (file_id,), one=True)
    if not f:
        abort(404)
    if f["stored_name"]:
        try:
            os.remove(os.path.join(UPLOAD_DIR, f["stored_name"]))
        except OSError:
            pass
    execute("DELETE FROM files WHERE id=?", (file_id,))
    flash("ลบเอกสารแนบแล้ว", "success")
    return redirect(request.form.get("next") or url_for("task_detail", task_id=f["task_id"]))


@app.route("/files")
def file_list():
    q = request.args.get("q", "").strip()
    kind = request.args.get("kind", "")
    task_id = request.args.get("task_id", type=int)
    sql = """SELECT f.*, t.title task_title, s.name step_name, s.seq step_seq
             FROM files f JOIN tasks t ON t.id=f.task_id
             LEFT JOIN steps s ON s.id=f.step_id WHERE 1=1"""
    args = []
    if q:
        sql += (" AND (f.subject LIKE ? OR f.doc_no LIKE ? OR f.counterparty LIKE ?"
                " OR f.filename LIKE ? OR t.title LIKE ?)")
        args += ["%%%s%%" % q] * 5
    if kind:
        sql += " AND f.kind = ?"
        args.append(kind)
    if task_id:
        sql += " AND f.task_id = ?"
        args.append(task_id)
    sql += " ORDER BY f.id DESC"
    files = query(sql, args)
    counts = {k: query("SELECT COUNT(*) c FROM files WHERE kind=?", (k,), one=True)["c"]
              for k in KINDS}
    return render_template("files.html", files=files, q=q, kind=kind, counts=counts,
                           task_id=task_id)


# ---------------------------------------------------------------------------
# Routes : Reports
# ---------------------------------------------------------------------------
@app.route("/reports")
def reports():
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    cond, args = "", []
    if start:
        cond += " AND date(created_at) >= ?"
        args.append(start)
    if end:
        cond += " AND date(created_at) <= ?"
        args.append(end)

    by_status = {s: 0 for s in STATUSES}
    for r in query("SELECT status, COUNT(*) c FROM tasks WHERE 1=1%s GROUP BY status" % cond, args):
        by_status[r["status"]] = r["c"]
    by_priority = {p: 0 for p in PRIORITIES}
    for r in query("SELECT priority, COUNT(*) c FROM tasks WHERE 1=1%s GROUP BY priority" % cond, args):
        by_priority[r["priority"]] = r["c"]
    by_category = query(
        """SELECT COALESCE(NULLIF(category,''),'ไม่ระบุ') k, COUNT(*) c,
                  SUM(CASE WHEN status='เสร็จสิ้น' THEN 1 ELSE 0 END) done
           FROM tasks WHERE 1=1%s GROUP BY k ORDER BY c DESC""" % cond, args)
    by_assignee = query(
        """SELECT COALESCE(NULLIF(assignee,''),'ไม่ระบุ') k, COUNT(*) c,
                  SUM(CASE WHEN status='เสร็จสิ้น' THEN 1 ELSE 0 END) done,
                  SUM(CASE WHEN due_date < date('now') AND status NOT IN ('เสร็จสิ้น','ยกเลิก')
                           THEN 1 ELSE 0 END) overdue,
                  AVG(progress) avg_progress
           FROM tasks WHERE 1=1%s GROUP BY k ORDER BY c DESC""" % cond, args)

    step_overdue = query(
        """SELECT s.*, t.title task_title, t.assignee FROM steps s JOIN tasks t ON t.id=s.task_id
           WHERE s.status <> 'เสร็จสิ้น' AND s.due_date IS NOT NULL AND s.due_date <> ''
             AND s.due_date < ? AND t.status NOT IN ('เสร็จสิ้น','ยกเลิก')
           ORDER BY s.due_date""", (today_str(),))
    step_stat = query(
        """SELECT status, COUNT(*) c FROM steps GROUP BY status""")
    steps_by_status = {s: 0 for s in STEP_STATUSES}
    for r in step_stat:
        steps_by_status[r["status"]] = r["c"]

    by_kind = {k: query("SELECT COUNT(*) c FROM files WHERE kind=?", (k,), one=True)["c"]
               for k in KINDS}
    file_month = query(
        """SELECT strftime('%Y-%m', COALESCE(doc_date, created_at)) m,
                  SUM(CASE WHEN kind='หนังสือเข้า' THEN 1 ELSE 0 END) cin,
                  SUM(CASE WHEN kind='หนังสือออก' THEN 1 ELSE 0 END) cout,
                  SUM(CASE WHEN kind='เอกสารประกอบ' THEN 1 ELSE 0 END) cdoc
           FROM files GROUP BY m ORDER BY m DESC LIMIT 12""")
    top_docs = query(
        """SELECT t.id, t.title, COUNT(f.id) c FROM tasks t JOIN files f ON f.task_id=t.id
           GROUP BY t.id ORDER BY c DESC LIMIT 8""")
    overdue = query(
        """SELECT * FROM tasks WHERE due_date IS NOT NULL AND due_date <> '' AND due_date < ?
             AND status NOT IN ('เสร็จสิ้น','ยกเลิก') ORDER BY due_date""", (today_str(),))

    total = sum(by_status.values())
    done = by_status.get("เสร็จสิ้น", 0)
    rate = round(done * 100.0 / total) if total else 0
    n_files = query("SELECT COUNT(*) c FROM files", one=True)["c"]
    with_file = query("SELECT COUNT(*) c FROM files WHERE stored_name IS NOT NULL", one=True)["c"]

    return render_template("reports.html", by_status=by_status, by_priority=by_priority,
                           by_category=by_category, by_assignee=by_assignee,
                           steps_by_status=steps_by_status, step_overdue=step_overdue,
                           by_kind=by_kind, file_month=file_month, top_docs=top_docs,
                           overdue=overdue, total=total, done=done, rate=rate,
                           start=start, end=end, n_files=n_files, with_file=with_file,
                           months=THAI_MONTHS_FULL)


@app.route("/reports/export/<kind>.csv")
def export_csv(kind):
    buf = io.StringIO()
    w = csv.writer(buf)
    if kind == "tasks":
        w.writerow(["รหัส", "ชื่องาน", "ประเภทงาน", "ผู้มอบหมาย", "ผู้รับผิดชอบ", "ความสำคัญ",
                    "สถานะ", "ความคืบหน้า(%)", "ขั้นตอนทั้งหมด", "ขั้นตอนที่เสร็จ", "เอกสารแนบ",
                    "วันเริ่ม", "กำหนดส่ง", "วันที่เสร็จ"])
        for t in query("""SELECT t.*,
                   (SELECT COUNT(*) FROM steps s WHERE s.task_id=t.id) ns,
                   (SELECT COUNT(*) FROM steps s WHERE s.task_id=t.id AND s.status='เสร็จสิ้น') nd,
                   (SELECT COUNT(*) FROM files f WHERE f.task_id=t.id) nf
                   FROM tasks t ORDER BY t.id"""):
            w.writerow([t["id"], t["title"], t["category"], t["owner"], t["assignee"],
                        t["priority"], t["status"], t["progress"], t["ns"], t["nd"], t["nf"],
                        t["start_date"] or "", t["due_date"] or "", t["done_date"] or ""])
        name = "tasks"
    elif kind == "steps":
        w.writerow(["รหัสงาน", "ชื่องาน", "ลำดับ", "ขั้นตอน", "ผู้รับผิดชอบ", "กำหนดส่ง",
                    "สถานะ", "วันที่เสร็จ", "เอกสารแนบ", "หมายเหตุ"])
        for s in query("""SELECT s.*, t.title,
                          (SELECT COUNT(*) FROM files f WHERE f.step_id=s.id) nf
                          FROM steps s JOIN tasks t ON t.id=s.task_id
                          ORDER BY s.task_id, s.seq"""):
            w.writerow([s["task_id"], s["title"], s["seq"], s["name"], s["owner"],
                        s["due_date"] or "", s["status"], s["done_date"] or "",
                        s["nf"], s["note"] or ""])
        name = "steps"
    elif kind == "files":
        w.writerow(["รหัส", "งาน", "ขั้นตอน", "ประเภทเอกสาร", "เลขที่หนังสือ", "ลงวันที่",
                    "เรื่อง", "จาก/ถึง", "ชื่อไฟล์", "ขนาด(ไบต์)", "ผู้แนบ", "แนบเมื่อ"])
        for f in query("""SELECT f.*, t.title, s.name sname FROM files f
                          JOIN tasks t ON t.id=f.task_id LEFT JOIN steps s ON s.id=f.step_id
                          ORDER BY f.id"""):
            w.writerow([f["id"], f["title"], f["sname"] or "", f["kind"], f["doc_no"] or "",
                        f["doc_date"] or "", f["subject"] or "", f["counterparty"] or "",
                        f["filename"] or "", f["filesize"] or "", f["uploader"] or "",
                        f["created_at"]])
        name = "files"
    else:
        abort(404)
    return Response("﻿" + buf.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=%s_%s.csv" % (name, today_str())})


@app.errorhandler(413)
def too_large(e):
    flash("ไฟล์รวมกันใหญ่เกิน 32 MB ต่อการอัปโหลด 1 ครั้ง กรุณาแบ่งอัปโหลดหลายครั้ง", "error")
    return redirect(request.referrer or url_for("dashboard"))


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------
BASE = """
<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{% block title %}ระบบติดตามงาน{% endblock %} · ระบบติดตามงาน</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@400;500;600;700&display=swap" rel="stylesheet">
<script>
tailwind.config = {
  theme: { extend: {
    fontFamily: { sans: ['Sarabun', 'system-ui', 'sans-serif'] },
    colors: {
      surface: '#fcfcfb', plane: '#f9f9f7',
      ink: { DEFAULT: '#0b0b0b', 2: '#52514e', 3: '#898781' },
      line: '#e1e0d9',
      brand: { DEFAULT: '#2a78d6', dark: '#256abf', light: '#cde2fb' },
      good: '#0ca30c', warn: '#fab219', serious: '#ec835a', critical: '#d03b3b',
    },
  } },
}
</script>
<style>
  body { background: #f9f9f7; }
  .tnum { font-variant-numeric: tabular-nums; }
  details > summary { list-style: none; cursor: pointer; }
  details > summary::-webkit-details-marker { display: none; }
  details[open] > summary .caret { transform: rotate(90deg); }
  .caret { display: inline-block; transition: transform .15s; }
  /* ข้อความภาษาไทยเป็นสายยาวไม่มีช่องว่าง ให้ตัดบรรทัดได้เพื่อไม่ให้ล้นจอเล็ก */
  h1, h2, h3 { overflow-wrap: anywhere; }
  :target { scroll-margin-top: 4.5rem; }
</style>
</head>
<body class="font-sans text-ink antialiased">

<header class="bg-white border-b border-line sticky top-0 z-20">
  <div class="max-w-7xl mx-auto px-4">
    <div class="flex items-center gap-3 h-14">
      <a href="{{ url_for('dashboard') }}" class="flex items-center gap-2 font-bold text-[15px] shrink-0">
        <span class="w-8 h-8 rounded-lg bg-brand text-white grid place-items-center text-sm">TD</span>
        <span class="hidden md:inline">ระบบติดตามงาน</span>
      </a>
      <nav class="ml-auto min-w-0 flex items-center gap-1 text-sm overflow-x-auto">
        {% set nav = [('dashboard','ภาพรวม'),('task_list','งานทั้งหมด'),('file_list','เอกสารแนบ'),('reports','สรุปรายงาน')] %}
        {% for ep, label in nav %}
          <a href="{{ url_for(ep) }}"
             class="px-3 py-1.5 rounded-lg transition shrink-0 whitespace-nowrap
             {{ 'bg-brand text-white font-medium' if request.endpoint == ep or (ep=='task_list' and request.endpoint in ['task_new','task_detail','task_edit']) else 'text-ink-2 hover:bg-plane' }}">
            {{ label }}
          </a>
        {% endfor %}
        <a href="{{ url_for('task_new') }}"
           class="ml-2 px-3 py-1.5 rounded-lg bg-ink text-white font-medium hover:bg-black shrink-0 whitespace-nowrap">+ สร้างงาน</a>
      </nav>
    </div>
  </div>
</header>

<main class="max-w-7xl mx-auto px-4 py-6">
  {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
      <div class="mb-4 space-y-2">
      {% for cat, msg in messages %}
        <div class="flex items-start gap-2 px-4 py-2.5 rounded-xl text-sm border
             {{ 'bg-red-50 border-red-200 text-red-800' if cat=='error' else 'bg-green-50 border-green-200 text-green-800' }}">
          <span>{{ '✕' if cat=='error' else '✓' }}</span><span>{{ msg }}</span>
        </div>
      {% endfor %}
      </div>
    {% endif %}
  {% endwith %}
  {% block content %}{% endblock %}
</main>

<footer class="max-w-7xl mx-auto px-4 py-8 text-xs text-ink-3">
  ระบบติดตามงาน · งาน → ขั้นตอน → เอกสารแนบ · Flask + SQLite · ข้อมูล ณ {{ now_display }}
</footer>
</body>
</html>
"""

MACROS = """
{% macro status_badge(s) -%}
  {% set m = {'รอดำเนินการ':'bg-slate-100 text-slate-700 ring-slate-200',
              'กำลังดำเนินการ':'bg-blue-50 text-blue-800 ring-blue-200',
              'รอตรวจสอบ':'bg-amber-50 text-amber-800 ring-amber-200',
              'เสร็จสิ้น':'bg-green-50 text-green-800 ring-green-200',
              'ยกเลิก':'bg-slate-100 text-slate-500 ring-slate-200'} %}
  <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ring-1 whitespace-nowrap {{ m.get(s,'bg-slate-100 text-slate-700 ring-slate-200') }}">{{ s }}</span>
{%- endmacro %}

{% macro priority_badge(p) -%}
  {% set m = {'ต่ำ':'bg-slate-100 text-slate-600 ring-slate-200',
              'ปกติ':'bg-sky-50 text-sky-800 ring-sky-200',
              'สูง':'bg-orange-50 text-orange-800 ring-orange-200',
              'ด่วนที่สุด':'bg-red-50 text-red-800 ring-red-200'} %}
  {% set icon = {'ต่ำ':'○','ปกติ':'◔','สูง':'◑','ด่วนที่สุด':'●'} %}
  <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ring-1 whitespace-nowrap {{ m.get(p,'') }}">
    <span aria-hidden="true">{{ icon.get(p,'') }}</span>{{ p }}</span>
{%- endmacro %}

{% macro kind_badge(k) -%}
  {% set m = {'หนังสือเข้า':'bg-blue-50 text-blue-800 ring-blue-200',
              'หนังสือออก':'bg-violet-50 text-violet-800 ring-violet-200',
              'เอกสารประกอบ':'bg-slate-100 text-slate-700 ring-slate-200'} %}
  {% set icon = {'หนังสือเข้า':'↓','หนังสือออก':'↑','เอกสารประกอบ':'▣'} %}
  <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ring-1 whitespace-nowrap {{ m.get(k,'') }}">
    <span aria-hidden="true">{{ icon.get(k,'') }}</span>{{ k }}</span>
{%- endmacro %}

{% macro due_cell(due, status) -%}
  {% set dl = days_left(due, status) %}
  <div class="tnum whitespace-nowrap">{{ due|thdate }}</div>
  {% if dl is not none %}
    {% if dl < 0 %}<div class="text-xs font-medium text-critical whitespace-nowrap">เกินกำหนด {{ -dl }} วัน</div>
    {% elif dl == 0 %}<div class="text-xs font-medium text-serious whitespace-nowrap">ครบกำหนดวันนี้</div>
    {% elif dl <= 7 %}<div class="text-xs text-amber-700 whitespace-nowrap">เหลือ {{ dl }} วัน</div>
    {% else %}<div class="text-xs text-ink-3 whitespace-nowrap">เหลือ {{ dl }} วัน</div>{% endif %}
  {% endif %}
{%- endmacro %}

{% macro progress_bar(v, color) -%}
  <div class="flex items-center gap-2">
    <div class="flex-1 h-2 rounded-full bg-line/70 overflow-hidden">
      <div class="h-2 rounded-r" style="width: {{ v }}%; background: {{ color or '#2a78d6' }}"></div>
    </div>
    <span class="text-xs tnum text-ink-2 w-9 text-right">{{ v }}%</span>
  </div>
{%- endmacro %}

{% macro bar_row(label, count, total, color) -%}
  <div class="flex items-center gap-3">
    <div class="w-32 shrink-0 text-sm text-ink-2 truncate" title="{{ label }}">{{ label }}</div>
    <div class="flex-1 h-3 rounded bg-plane ring-1 ring-line/70 overflow-hidden">
      <div class="h-3 rounded-r-[4px]" style="width: {{ (count * 100 / total) if total else 0 }}%; background: {{ color }}"></div>
    </div>
    <div class="w-8 text-right text-sm tnum font-medium">{{ count }}</div>
  </div>
{%- endmacro %}

{% macro kpi(label, value, hint, accent) -%}
  <div class="bg-white rounded-2xl border border-line p-4">
    <div class="text-sm text-ink-2">{{ label }}</div>
    <div class="mt-1 text-3xl font-bold" style="color: {{ accent or '#0b0b0b' }}">{{ value }}</div>
    <div class="mt-0.5 text-xs text-ink-3">{{ hint }}</div>
  </div>
{%- endmacro %}

{# ฟอร์มแนบเอกสาร ใช้ซ้ำได้ทั้งระดับงานและระดับขั้นตอน แนบได้หลายไฟล์ต่อครั้ง #}
{% macro upload_form(task_id, step_id, compact) -%}
<form method="post" action="{{ url_for('file_upload', task_id=task_id) }}"
      enctype="multipart/form-data" class="space-y-2.5">
  {% if step_id %}<input type="hidden" name="step_id" value="{{ step_id }}">{% endif %}
  <div class="grid sm:grid-cols-3 gap-2">
    <select name="kind" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
      {% for k in KINDS %}<option value="{{ k }}">{{ k }}</option>{% endfor %}
    </select>
    <input name="doc_no" placeholder="เลขที่หนังสือ (ถ้ามี)"
           class="px-3 py-2 rounded-lg border border-line bg-plane text-sm">
    <input type="date" name="doc_date" value="{{ today_str() }}"
           class="px-3 py-2 rounded-lg border border-line bg-plane text-sm tnum">
  </div>
  <div class="grid sm:grid-cols-2 gap-2">
    <input name="subject" placeholder="เรื่อง / ชื่อเอกสาร"
           class="px-3 py-2 rounded-lg border border-line bg-plane text-sm">
    <input name="counterparty" placeholder="จาก / ถึง หน่วยงาน"
           class="px-3 py-2 rounded-lg border border-line bg-plane text-sm">
  </div>
  {% if not compact %}
  <input name="note" placeholder="หมายเหตุ"
         class="w-full px-3 py-2 rounded-lg border border-line bg-plane text-sm">
  {% endif %}
  <input type="file" name="files" multiple
         class="w-full text-sm file:mr-3 file:px-3 file:py-1.5 file:rounded-lg file:border-0 file:bg-brand file:text-white file:font-medium file:cursor-pointer border border-line rounded-lg bg-plane p-1.5">
  <div class="flex items-center gap-2">
    <input name="uploader" placeholder="ผู้แนบ"
           class="w-36 px-3 py-2 rounded-lg border border-line bg-plane text-sm">
    <button class="px-4 py-2 rounded-lg bg-brand text-white text-sm font-medium hover:bg-brand-dark">แนบเอกสาร</button>
    <span class="text-xs text-ink-3">เลือกได้หลายไฟล์ และแนบเพิ่มได้ทุกเมื่อ</span>
  </div>
</form>
{%- endmacro %}

{% macro file_item(f, next_url) -%}
<div class="flex items-start gap-2 py-2">
  {{ kind_badge(f['kind']) }}
  <div class="min-w-0 flex-1">
    <div class="text-sm font-medium">{{ f['subject'] or f['filename'] or 'ไม่ระบุเรื่อง' }}</div>
    <div class="text-xs text-ink-3">
      {% if f['doc_no'] %}{{ f['doc_no'] }} · {% endif %}
      {% if f['doc_date'] %}{{ f['doc_date']|thdate }} · {% endif %}
      {% if f['counterparty'] %}{{ f['counterparty'] }} · {% endif %}
      {{ f['created_at']|thdatetime }}{% if f['uploader'] %} · {{ f['uploader'] }}{% endif %}
    </div>
    {% if f['note'] %}<div class="text-xs text-ink-3 italic">{{ f['note'] }}</div>{% endif %}
    {% if f['stored_name'] %}
      <a href="{{ url_for('file_download', file_id=f['id']) }}"
         class="mt-1 inline-flex items-center gap-1 text-xs text-brand hover:underline">
        ↓ {{ f['filename'] }} <span class="text-ink-3">({{ f['filesize']|filesize }})</span></a>
    {% else %}
      <div class="mt-0.5 text-xs text-ink-3">— ยังไม่มีไฟล์แนบ (บันทึกข้อมูลเอกสารไว้เท่านั้น)</div>
    {% endif %}
  </div>
  <form method="post" action="{{ url_for('file_delete', file_id=f['id']) }}"
        onsubmit="return confirm('ลบเอกสารนี้?')" class="shrink-0">
    <input type="hidden" name="next" value="{{ next_url }}">
    <button class="text-xs text-ink-3 hover:text-critical px-1.5 py-0.5 rounded hover:bg-red-50">ลบ</button>
  </form>
</div>
{%- endmacro %}
"""

DASHBOARD = """
{% extends "base.html" %}{% from "macros.html" import kpi, bar_row, status_badge, due_cell, progress_bar, kind_badge %}
{% block title %}ภาพรวม{% endblock %}
{% block content %}
<div class="flex items-end justify-between mb-4">
  <div>
    <h1 class="text-2xl font-bold">ภาพรวมการติดตามงาน</h1>
    <p class="text-sm text-ink-2 mt-0.5">สถานะงาน ขั้นตอนที่ต้องเร่ง และเอกสารที่แนบล่าสุด</p>
  </div>
  <a href="{{ url_for('reports') }}" class="text-sm text-brand hover:underline">ดูสรุปรายงานทั้งหมด →</a>
</div>

<div class="grid grid-cols-2 lg:grid-cols-5 gap-3 mb-5">
  {{ kpi('งานทั้งหมด', total, 'รายการในระบบ', none) }}
  {{ kpi('กำลังดำเนินการ', doing, 'อยู่ระหว่างทำ', '#2a78d6') }}
  {{ kpi('เกินกำหนด', overdue|length, 'ต้องเร่งติดตาม', '#d03b3b') }}
  {{ kpi('เสร็จสิ้น', done, 'ปิดงานแล้ว', '#0ca30c') }}
  {{ kpi('เอกสารแนบ', n_files, 'รายการทุกงาน', none) }}
</div>

<div class="grid lg:grid-cols-2 gap-4 mb-4">
  <section class="bg-white rounded-2xl border border-line p-5">
    <h2 class="font-semibold mb-1">จำนวนงานตามสถานะ</h2>
    <p class="text-xs text-ink-3 mb-4">หน่วย: รายการ</p>
    <div class="space-y-2.5">
      {% set cmap = {'รอดำเนินการ':'#898781','กำลังดำเนินการ':'#2a78d6','รอตรวจสอบ':'#fab219','เสร็จสิ้น':'#0ca30c','ยกเลิก':'#c3c2b7'} %}
      {% for s in STATUSES %}{{ bar_row(s, by_status[s], total, cmap[s]) }}{% endfor %}
    </div>
  </section>
  <section class="bg-white rounded-2xl border border-line p-5">
    <h2 class="font-semibold mb-1">จำนวนงานตามระดับความสำคัญ</h2>
    <p class="text-xs text-ink-3 mb-4">หน่วย: รายการ</p>
    <div class="space-y-2.5">
      {% set pmap = {'ต่ำ':'#898781','ปกติ':'#2a78d6','สูง':'#fab219','ด่วนที่สุด':'#d03b3b'} %}
      {% for p in PRIORITIES %}{{ bar_row(p, by_priority[p], total, pmap[p]) }}{% endfor %}
    </div>
  </section>
</div>

<div class="grid lg:grid-cols-2 gap-4 mb-4">
  <section class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-5 py-3.5 border-b border-line flex items-center gap-2">
      <span class="w-2 h-2 rounded-full bg-critical"></span>
      <h2 class="font-semibold">งานเกินกำหนด</h2>
      <span class="text-xs text-ink-3">{{ overdue|length }} รายการ</span>
      <a href="{{ url_for('task_list', view='overdue') }}" class="ml-auto text-xs text-brand hover:underline">ดูทั้งหมด</a>
    </div>
    {% if overdue %}
    <ul class="divide-y divide-line">
      {% for t in overdue[:5] %}
      <li class="px-5 py-3 flex items-center gap-3 hover:bg-plane">
        <div class="min-w-0 flex-1">
          <a href="{{ url_for('task_detail', task_id=t['id']) }}" class="font-medium hover:text-brand line-clamp-1">{{ t['title'] }}</a>
          <div class="text-xs text-ink-3 mt-0.5">{{ t['assignee'] or 'ไม่ระบุผู้รับผิดชอบ' }} · {{ t['category'] or '-' }}</div>
        </div>
        <div class="text-right text-sm shrink-0">{{ due_cell(t['due_date'], t['status']) }}</div>
      </li>
      {% endfor %}
    </ul>
    {% else %}<p class="px-5 py-8 text-center text-sm text-ink-3">ไม่มีงานเกินกำหนด</p>{% endif %}
  </section>

  <section class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-5 py-3.5 border-b border-line flex items-center gap-2">
      <span class="w-2 h-2 rounded-full bg-warn"></span>
      <h2 class="font-semibold">ขั้นตอนที่ถึงกำหนดใน 7 วัน</h2>
      <span class="text-xs text-ink-3">{{ steps_due|length }} ขั้นตอน</span>
    </div>
    {% if steps_due %}
    <ul class="divide-y divide-line">
      {% for s in steps_due %}
      <li class="px-5 py-3 flex items-center gap-3 hover:bg-plane">
        <span class="w-6 h-6 rounded-lg bg-plane ring-1 ring-line grid place-items-center text-xs tnum shrink-0">{{ s['seq'] }}</span>
        <div class="min-w-0 flex-1">
          <div class="font-medium text-sm line-clamp-1">{{ s['name'] }}</div>
          <a href="{{ url_for('task_detail', task_id=s['task_id']) }}#steps"
             class="text-xs text-ink-3 hover:text-brand line-clamp-1">{{ s['task_title'] }}</a>
        </div>
        <div class="text-right text-sm shrink-0">{{ due_cell(s['due_date'], s['status']) }}</div>
      </li>
      {% endfor %}
    </ul>
    {% else %}<p class="px-5 py-8 text-center text-sm text-ink-3">ไม่มีขั้นตอนที่ใกล้ครบกำหนด</p>{% endif %}
  </section>
</div>

<div class="grid lg:grid-cols-2 gap-4">
  <section class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-5 py-3.5 border-b border-line flex items-center">
      <h2 class="font-semibold">เอกสารที่แนบล่าสุด</h2>
      <a href="{{ url_for('file_list') }}" class="ml-auto text-xs text-brand hover:underline">ดูเอกสารทั้งหมด</a>
    </div>
    <ul class="divide-y divide-line">
      {% for f in recent_files %}
      <li class="px-5 py-3 flex items-start gap-2">
        {{ kind_badge(f['kind']) }}
        <div class="min-w-0 flex-1">
          <div class="font-medium text-sm line-clamp-1">{{ f['subject'] or f['filename'] }}</div>
          <a href="{{ url_for('task_detail', task_id=f['task_id']) }}" class="text-xs text-ink-3 hover:text-brand line-clamp-1">
            {{ f['task_title'] }}{% if f['step_name'] %} › {{ f['step_name'] }}{% endif %}</a>
        </div>
        {% if f['stored_name'] %}
        <a href="{{ url_for('file_download', file_id=f['id']) }}" class="text-xs text-brand hover:underline shrink-0">↓ ไฟล์</a>
        {% endif %}
      </li>
      {% else %}<li class="px-5 py-8 text-center text-sm text-ink-3">ยังไม่มีเอกสารแนบ</li>{% endfor %}
    </ul>
  </section>

  <section class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-5 py-3.5 border-b border-line"><h2 class="font-semibold">รายงานความคืบหน้าล่าสุด</h2></div>
    <ul class="divide-y divide-line">
      {% for u in recent_updates %}
      <li class="px-5 py-3">
        <a href="{{ url_for('task_detail', task_id=u['task_id']) }}" class="font-medium text-sm hover:text-brand line-clamp-1">
          {{ u['title'] }}{% if u['step_name'] %} › {{ u['step_name'] }}{% endif %}</a>
        <p class="text-sm text-ink-2 mt-0.5 line-clamp-2">{{ u['note'] }}</p>
        <div class="text-xs text-ink-3 mt-1">{{ u['reporter'] or 'ไม่ระบุ' }} · {{ u['created_at']|thdatetime }}</div>
      </li>
      {% else %}<li class="px-5 py-8 text-center text-sm text-ink-3">ยังไม่มีรายงาน</li>{% endfor %}
    </ul>
  </section>
</div>
{% endblock %}
"""

TASKS = """
{% extends "base.html" %}{% from "macros.html" import status_badge, priority_badge, due_cell, progress_bar %}
{% block title %}งานทั้งหมด{% endblock %}
{% block content %}
<div class="flex items-end justify-between mb-4">
  <div>
    <h1 class="text-2xl font-bold">งานทั้งหมด</h1>
    <p class="text-sm text-ink-2 mt-0.5">พบ {{ tasks|length }} รายการ</p>
  </div>
  <a href="{{ url_for('task_new') }}" class="px-4 py-2 rounded-xl bg-ink text-white text-sm font-medium hover:bg-black">+ สร้างงานใหม่</a>
</div>

<form method="get" class="bg-white rounded-2xl border border-line p-3 mb-4 flex flex-wrap items-center gap-2">
  <input name="q" value="{{ q }}" placeholder="ค้นหาชื่องาน / รายละเอียด / ผู้รับผิดชอบ"
         class="flex-1 min-w-[220px] px-3 py-2 rounded-lg border border-line bg-plane text-sm focus:outline-none focus:ring-2 focus:ring-brand/40">
  <select name="status" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
    <option value="">ทุกสถานะ</option>
    {% for s in STATUSES %}<option value="{{ s }}" {{ 'selected' if status==s }}>{{ s }}</option>{% endfor %}
  </select>
  <select name="priority" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
    <option value="">ทุกความสำคัญ</option>
    {% for p in PRIORITIES %}<option value="{{ p }}" {{ 'selected' if priority==p }}>{{ p }}</option>{% endfor %}
  </select>
  <select name="assignee" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
    <option value="">ทุกผู้รับผิดชอบ</option>
    {% for a in assignees %}<option value="{{ a }}" {{ 'selected' if assignee==a }}>{{ a }}</option>{% endfor %}
  </select>
  <select name="view" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
    <option value="">ทั้งหมด</option>
    <option value="open" {{ 'selected' if view=='open' }}>เฉพาะที่ยังไม่ปิด</option>
    <option value="overdue" {{ 'selected' if view=='overdue' }}>เฉพาะเกินกำหนด</option>
  </select>
  <button class="px-4 py-2 rounded-lg bg-brand text-white text-sm font-medium hover:bg-brand-dark">ค้นหา</button>
  <a href="{{ url_for('task_list') }}" class="px-3 py-2 rounded-lg text-sm text-ink-2 hover:bg-plane">ล้าง</a>
</form>

<div class="bg-white rounded-2xl border border-line overflow-hidden">
  <div class="overflow-x-auto">
  <table class="w-full text-sm">
    <thead class="bg-plane text-ink-2 text-left">
      <tr>
        <th class="px-4 py-3 font-medium w-12">#</th>
        <th class="px-4 py-3 font-medium">ชื่องาน</th>
        <th class="px-4 py-3 font-medium">ผู้รับผิดชอบ</th>
        <th class="px-4 py-3 font-medium">ความสำคัญ</th>
        <th class="px-4 py-3 font-medium">สถานะ</th>
        <th class="px-4 py-3 font-medium whitespace-nowrap">ขั้นตอน</th>
        <th class="px-4 py-3 font-medium whitespace-nowrap">เอกสาร</th>
        <th class="px-4 py-3 font-medium w-40">ความคืบหน้า</th>
        <th class="px-4 py-3 font-medium">กำหนดส่ง</th>
      </tr>
    </thead>
    <tbody class="divide-y divide-line">
      {% for t in tasks %}
      {% set dl = days_left(t['due_date'], t['status']) %}
      <tr class="hover:bg-plane {{ 'bg-red-50/40' if dl is not none and dl < 0 }}">
        <td class="px-4 py-3 text-ink-3 tnum">{{ t['id'] }}</td>
        <td class="px-4 py-3">
          <a href="{{ url_for('task_detail', task_id=t['id']) }}" class="font-medium hover:text-brand">{{ t['title'] }}</a>
          <div class="text-xs text-ink-3">{{ t['category'] or '-' }}</div>
        </td>
        <td class="px-4 py-3 whitespace-nowrap">{{ t['assignee'] or '-' }}</td>
        <td class="px-4 py-3">{{ priority_badge(t['priority']) }}</td>
        <td class="px-4 py-3">{{ status_badge(t['status']) }}</td>
        <td class="px-4 py-3 tnum whitespace-nowrap">
          {% if t['n_steps'] %}{{ t['n_done'] }}/{{ t['n_steps'] }}{% else %}<span class="text-ink-3">-</span>{% endif %}
        </td>
        <td class="px-4 py-3 tnum">
          {% if t['n_files'] %}
          <a href="{{ url_for('file_list', task_id=t['id']) }}" class="text-brand hover:underline">{{ t['n_files'] }}</a>
          {% else %}<span class="text-ink-3">0</span>{% endif %}
        </td>
        <td class="px-4 py-3">{{ progress_bar(t['progress'], '#2a78d6') }}</td>
        <td class="px-4 py-3">{{ due_cell(t['due_date'], t['status']) }}</td>
      </tr>
      {% else %}
      <tr><td colspan="9" class="px-4 py-12 text-center text-ink-3">ไม่พบงานตามเงื่อนไขที่เลือก</td></tr>
      {% endfor %}
    </tbody>
  </table>
  </div>
</div>
{% endblock %}
"""

TASK_FORM = """
{% extends "base.html" %}
{% block title %}{{ 'สร้างงานใหม่' if mode=='new' else 'แก้ไขงาน' }}{% endblock %}
{% block content %}
<div class="max-w-3xl mx-auto">
  <h1 class="text-2xl font-bold mb-1">{{ 'สร้างงานใหม่' if mode=='new' else 'แก้ไขงาน' }}</h1>
  <p class="text-sm text-ink-2 mb-5">กรอกรายละเอียดงาน ผู้รับผิดชอบ และกำหนดส่ง{{ ' (กำหนดขั้นตอนเริ่มต้นได้ในช่องล่างสุด)' if mode=='new' }}</p>

  <form method="post" class="bg-white rounded-2xl border border-line p-6 space-y-5">
    <div>
      <label class="block text-sm font-medium mb-1.5">ชื่องาน <span class="text-critical">*</span></label>
      <input name="title" required value="{{ task['title'] if task else '' }}"
             class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane focus:outline-none focus:ring-2 focus:ring-brand/40"
             placeholder="เช่น จัดทำรายงานผลการดำเนินงานประจำไตรมาส">
    </div>
    <div>
      <label class="block text-sm font-medium mb-1.5">รายละเอียด</label>
      <textarea name="description" rows="3"
        class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane focus:outline-none focus:ring-2 focus:ring-brand/40">{{ task['description'] if task else '' }}</textarea>
    </div>

    <div class="grid sm:grid-cols-2 gap-4">
      <div>
        <label class="block text-sm font-medium mb-1.5">ประเภทงาน</label>
        <select name="category" class="w-full px-3 py-2.5 rounded-xl border border-line bg-white">
          {% for c in CATEGORIES %}<option value="{{ c }}" {{ 'selected' if task and task['category']==c }}>{{ c }}</option>{% endfor %}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium mb-1.5">ระดับความสำคัญ</label>
        <select name="priority" class="w-full px-3 py-2.5 rounded-xl border border-line bg-white">
          {% for p in PRIORITIES %}<option value="{{ p }}" {{ 'selected' if (task and task['priority']==p) or (not task and p=='ปกติ') }}>{{ p }}</option>{% endfor %}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium mb-1.5">ผู้มอบหมาย</label>
        <input name="owner" value="{{ task['owner'] if task else '' }}" class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane">
      </div>
      <div>
        <label class="block text-sm font-medium mb-1.5">ผู้รับผิดชอบ</label>
        <input name="assignee" value="{{ task['assignee'] if task else '' }}" class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane">
      </div>
      <div>
        <label class="block text-sm font-medium mb-1.5">วันที่เริ่ม</label>
        <input type="date" name="start_date" value="{{ task['start_date'] if task else today_str() }}"
               class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane tnum">
      </div>
      <div>
        <label class="block text-sm font-medium mb-1.5">กำหนดส่ง</label>
        <input type="date" name="due_date" value="{{ task['due_date'] if task else '' }}"
               class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane tnum">
      </div>
      <div>
        <label class="block text-sm font-medium mb-1.5">สถานะ</label>
        <select name="status" class="w-full px-3 py-2.5 rounded-xl border border-line bg-white">
          {% for s in STATUSES %}<option value="{{ s }}" {{ 'selected' if (task and task['status']==s) or (not task and s=='รอดำเนินการ') }}>{{ s }}</option>{% endfor %}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium mb-1.5">ความคืบหน้า (%)</label>
        <input type="number" name="progress" min="0" max="100" step="5" value="{{ task['progress'] if task else 0 }}"
               class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane tnum">
        <p class="mt-1 text-xs text-ink-3">ถ้างานมีขั้นตอน ระบบจะคำนวณ % ให้อัตโนมัติจากขั้นตอนที่เสร็จ</p>
      </div>
    </div>

    {% if mode=='new' %}
    <div>
      <label class="block text-sm font-medium mb-1.5">ขั้นตอนการดำเนินงาน (ไม่บังคับ)</label>
      <textarea name="steps_text" rows="5" placeholder="พิมพ์บรรทัดละ 1 ขั้นตอน เช่น&#10;รับหนังสือสั่งการ&#10;จัดทำร่างเอกสาร&#10;เสนอผู้บริหารลงนาม&#10;จัดส่งและติดตามผล"
        class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane text-sm">{{ steps_text }}</textarea>
      <p class="mt-1 text-xs text-ink-3">เพิ่ม/แก้ไขขั้นตอน และแนบหนังสือของแต่ละขั้นตอนได้ในหน้ารายละเอียดงาน</p>
    </div>
    {% endif %}

    <div class="flex items-center gap-2 pt-2">
      <button class="px-5 py-2.5 rounded-xl bg-brand text-white font-medium hover:bg-brand-dark">บันทึก</button>
      <a href="{{ url_for('task_detail', task_id=task['id']) if task and task['id'] else url_for('task_list') }}"
         class="px-5 py-2.5 rounded-xl border border-line text-ink-2 hover:bg-plane">ยกเลิก</a>
    </div>
  </form>
</div>
{% endblock %}
"""

TASK_DETAIL = """
{% extends "base.html" %}
{% from "macros.html" import status_badge, priority_badge, due_cell, progress_bar, upload_form, file_item, kind_badge %}
{% block title %}{{ task['title'] }}{% endblock %}
{% block content %}
{% set here = url_for('task_detail', task_id=task['id']) %}
<div class="mb-4 text-sm text-ink-3">
  <a href="{{ url_for('task_list') }}" class="hover:text-brand">งานทั้งหมด</a> / งาน #{{ task['id'] }}
</div>

<div class="grid lg:grid-cols-3 gap-4">
<div class="lg:col-span-2 min-w-0 space-y-4">

  <section class="bg-white rounded-2xl border border-line p-6">
    <div class="flex flex-wrap items-start gap-3">
      <div class="min-w-0 flex-1">
        <h1 class="text-2xl font-bold">{{ task['title'] }}</h1>
        <div class="flex flex-wrap items-center gap-2 mt-2">
          {{ status_badge(task['status']) }}{{ priority_badge(task['priority']) }}
          <span class="text-xs text-ink-3">{{ task['category'] or 'ไม่ระบุประเภท' }}</span>
        </div>
      </div>
      <div class="ml-auto flex gap-2 shrink-0">
        <a href="{{ url_for('task_edit', task_id=task['id']) }}" class="px-3 py-1.5 rounded-lg border border-line text-sm hover:bg-plane">แก้ไข</a>
        <form method="post" action="{{ url_for('task_delete', task_id=task['id']) }}"
              onsubmit="return confirm('ยืนยันการลบงานนี้? ขั้นตอนและเอกสารแนบทั้งหมดจะถูกลบด้วย')">
          <button class="px-3 py-1.5 rounded-lg border border-red-200 text-critical text-sm hover:bg-red-50">ลบ</button>
        </form>
      </div>
    </div>

    {% if task['description'] %}<p class="mt-4 text-ink-2 whitespace-pre-line">{{ task['description'] }}</p>{% endif %}

    <div class="mt-5 max-w-sm">{{ progress_bar(task['progress'], '#2a78d6') }}</div>

    <dl class="mt-5 grid sm:grid-cols-2 gap-x-6 gap-y-3 text-sm border-t border-line pt-5">
      <div class="flex gap-2"><dt class="text-ink-3 w-28">ผู้มอบหมาย</dt><dd class="font-medium">{{ task['owner'] or '-' }}</dd></div>
      <div class="flex gap-2"><dt class="text-ink-3 w-28">ผู้รับผิดชอบ</dt><dd class="font-medium">{{ task['assignee'] or '-' }}</dd></div>
      <div class="flex gap-2"><dt class="text-ink-3 w-28">วันที่เริ่ม</dt><dd class="tnum">{{ task['start_date']|thdate }}</dd></div>
      <div class="flex gap-2"><dt class="text-ink-3 w-28">กำหนดส่ง</dt><dd>{{ due_cell(task['due_date'], task['status']) }}</dd></div>
      <div class="flex gap-2"><dt class="text-ink-3 w-28">วันที่เสร็จ</dt><dd class="tnum">{{ task['done_date']|thdate }}</dd></div>
      <div class="flex gap-2"><dt class="text-ink-3 w-28">แก้ไขล่าสุด</dt><dd class="tnum">{{ task['updated_at']|thdatetime }}</dd></div>
    </dl>
  </section>

  <!-- ขั้นตอนการดำเนินงาน -->
  <section id="steps" class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-6 py-4 border-b border-line flex items-center gap-2">
      <h2 class="font-semibold">ขั้นตอนการดำเนินงาน</h2>
      {% set ndone = steps|selectattr('status','equalto','เสร็จสิ้น')|list|length %}
      <span class="text-xs text-ink-3">เสร็จแล้ว {{ ndone }} จาก {{ steps|length }} ขั้นตอน</span>
    </div>

    <ol class="divide-y divide-line">
      {% for s in steps %}
      {% set sfiles = files_by_step.get(s['id'], []) %}
      <li id="step-{{ s['id'] }}" class="px-6 py-4 {{ 'bg-green-50/30' if s['status']=='เสร็จสิ้น' }}">
        <div class="flex items-start gap-3">
          <span class="w-7 h-7 rounded-lg grid place-items-center text-xs font-medium shrink-0 tnum
            {{ 'bg-good text-white' if s['status']=='เสร็จสิ้น' else ('bg-brand text-white' if s['status']=='กำลังดำเนินการ' else 'bg-plane ring-1 ring-line text-ink-2') }}">
            {{ '✓' if s['status']=='เสร็จสิ้น' else s['seq'] }}</span>

          <div class="min-w-0 flex-1">
            <div class="flex flex-wrap items-center gap-2">
              <span class="font-medium {{ 'line-through text-ink-3' if s['status']=='เสร็จสิ้น' }}">{{ s['name'] }}</span>
              {{ status_badge(s['status']) }}
              {% if sfiles %}<span class="text-xs text-ink-3">📎 {{ sfiles|length }} เอกสาร</span>{% endif %}
            </div>
            <div class="text-xs text-ink-3 mt-0.5">
              {{ s['owner'] or 'ไม่ระบุผู้รับผิดชอบ' }}
              {% if s['due_date'] %} · กำหนด {{ s['due_date']|thdate }}
                {% set dl = days_left(s['due_date'], s['status']) %}
                {% if dl is not none and dl < 0 %}<span class="text-critical font-medium">(เกิน {{ -dl }} วัน)</span>
                {% elif dl is not none and dl <= 3 %}<span class="text-amber-700">(เหลือ {{ dl }} วัน)</span>{% endif %}
              {% endif %}
              {% if s['done_date'] %} · เสร็จ {{ s['done_date']|thdate }}{% endif %}
            </div>
            {% if s['note'] %}<p class="text-xs text-ink-2 mt-1">{{ s['note'] }}</p>{% endif %}

            {% if sfiles %}
            <div class="mt-2 divide-y divide-line/70 border-l-2 border-line pl-3">
              {% for f in sfiles %}{{ file_item(f, here + '#step-' ~ s['id']) }}{% endfor %}
            </div>
            {% endif %}

            <div class="mt-2 flex flex-wrap items-center gap-2">
              <details class="w-full">
                <summary class="inline-flex items-center gap-1 text-xs text-brand hover:underline">
                  <span class="caret">›</span> แนบหนังสือ/ไฟล์ในขั้นตอนนี้
                </summary>
                <div class="mt-3 p-3 rounded-xl bg-plane border border-line">
                  {{ upload_form(task['id'], s['id'], true) }}
                </div>
              </details>
              <details class="w-full">
                <summary class="inline-flex items-center gap-1 text-xs text-ink-3 hover:text-ink">
                  <span class="caret">›</span> แก้ไขรายละเอียดขั้นตอน
                </summary>
                <form method="post" action="{{ url_for('step_update', step_id=s['id']) }}"
                      class="mt-3 p-3 rounded-xl bg-plane border border-line grid sm:grid-cols-2 gap-2">
                  <input name="name" value="{{ s['name'] }}" class="px-3 py-2 rounded-lg border border-line bg-white text-sm sm:col-span-2">
                  <input name="owner" value="{{ s['owner'] or '' }}" placeholder="ผู้รับผิดชอบ" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
                  <input type="date" name="due_date" value="{{ s['due_date'] or '' }}" class="px-3 py-2 rounded-lg border border-line bg-white text-sm tnum">
                  <select name="status" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
                    {% for st in STEP_STATUSES %}<option value="{{ st }}" {{ 'selected' if s['status']==st }}>{{ st }}</option>{% endfor %}
                  </select>
                  <input name="note" value="{{ s['note'] or '' }}" placeholder="หมายเหตุ" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
                  <div class="sm:col-span-2 flex items-center gap-2">
                    <button class="px-4 py-2 rounded-lg bg-brand text-white text-sm font-medium hover:bg-brand-dark">บันทึก</button>
                  </div>
                </form>
              </details>
            </div>
          </div>

          <div class="flex flex-col items-end gap-1 shrink-0">
            <div class="flex gap-1">
              {% for st, label in [('กำลังดำเนินการ','เริ่ม'), ('เสร็จสิ้น','เสร็จ')] %}
                {% if s['status'] != st %}
                <form method="post" action="{{ url_for('step_status', step_id=s['id'], status=st) }}">
                  <button class="px-2 py-1 rounded-lg border border-line text-xs hover:bg-plane whitespace-nowrap">{{ label }}</button>
                </form>
                {% endif %}
              {% endfor %}
            </div>
            <div class="flex gap-1">
              <form method="post" action="{{ url_for('step_move', step_id=s['id'], direction='up') }}">
                <button class="px-1.5 py-0.5 rounded text-xs text-ink-3 hover:bg-plane" title="เลื่อนขึ้น">↑</button>
              </form>
              <form method="post" action="{{ url_for('step_move', step_id=s['id'], direction='down') }}">
                <button class="px-1.5 py-0.5 rounded text-xs text-ink-3 hover:bg-plane" title="เลื่อนลง">↓</button>
              </form>
              <form method="post" action="{{ url_for('step_delete', step_id=s['id']) }}" onsubmit="return confirm('ลบขั้นตอนนี้?')">
                <button class="px-1.5 py-0.5 rounded text-xs text-ink-3 hover:text-critical hover:bg-red-50">ลบ</button>
              </form>
            </div>
          </div>
        </div>
      </li>
      {% else %}
      <li class="px-6 py-8 text-center text-sm text-ink-3">ยังไม่มีขั้นตอน — เพิ่มขั้นตอนแรกได้ด้านล่าง</li>
      {% endfor %}
    </ol>

    <form method="post" action="{{ url_for('step_add', task_id=task['id']) }}"
          class="px-6 py-4 bg-plane border-t border-line grid sm:grid-cols-12 gap-2">
      <input name="name" required placeholder="+ เพิ่มขั้นตอนใหม่" class="sm:col-span-5 px-3 py-2 rounded-lg border border-line bg-white text-sm">
      <input name="owner" placeholder="ผู้รับผิดชอบ" class="sm:col-span-3 px-3 py-2 rounded-lg border border-line bg-white text-sm">
      <input type="date" name="due_date" class="sm:col-span-2 px-3 py-2 rounded-lg border border-line bg-white text-sm tnum">
      <button class="sm:col-span-2 px-4 py-2 rounded-lg bg-ink text-white text-sm font-medium hover:bg-black">เพิ่ม</button>
    </form>
  </section>

  <!-- รายงานความคืบหน้า -->
  <section id="updates" class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-6 py-4 border-b border-line flex items-center">
      <h2 class="font-semibold">รายงานความคืบหน้า</h2>
      <span class="ml-2 text-xs text-ink-3">{{ updates|length }} ครั้ง</span>
    </div>
    <ol class="divide-y divide-line">
      {% for u in updates %}
      <li class="px-6 py-4">
        <div class="flex flex-wrap items-center gap-2 text-xs text-ink-3">
          <span class="font-medium text-ink-2">{{ u['reporter'] or 'ไม่ระบุผู้รายงาน' }}</span>
          <span>·</span><span class="tnum">{{ u['created_at']|thdatetime }}</span>
          {% if u['step_name'] %}<span>·</span><span class="px-2 py-0.5 rounded-full bg-plane ring-1 ring-line">ขั้นตอน: {{ u['step_name'] }}</span>{% endif %}
        </div>
        <p class="mt-1.5 text-ink-2 whitespace-pre-line">{{ u['note'] }}</p>
        {% set ufiles = files|selectattr('update_id','equalto',u['id'])|list %}
        {% if ufiles %}
        <div class="mt-2 flex flex-wrap gap-2">
          {% for f in ufiles %}
          {% if f['stored_name'] %}
          <a href="{{ url_for('file_download', file_id=f['id']) }}"
             class="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-plane ring-1 ring-line text-xs text-brand hover:bg-white">
            ↓ {{ f['filename'] }} <span class="text-ink-3">{{ f['filesize']|filesize }}</span></a>
          {% endif %}
          {% endfor %}
        </div>
        {% endif %}
      </li>
      {% else %}
      <li class="px-6 py-8 text-center text-sm text-ink-3">ยังไม่มีการรายงานความคืบหน้า</li>
      {% endfor %}
    </ol>
  </section>
</div>

<!-- คอลัมน์ขวา -->
<div class="min-w-0 space-y-4">
  <section class="bg-white rounded-2xl border border-line p-5">
    <h2 class="font-semibold mb-3">รายงานความคืบหน้า + แนบไฟล์</h2>
    <form method="post" action="{{ url_for('task_report', task_id=task['id']) }}"
          enctype="multipart/form-data" class="space-y-3">
      <select name="step_id" class="w-full px-3 py-2 rounded-lg border border-line bg-white text-sm">
        <option value="">— รายงานภาพรวมของงาน —</option>
        {% for s in steps %}<option value="{{ s['id'] }}">ขั้นตอน {{ s['seq'] }}: {{ s['name'] }}</option>{% endfor %}
      </select>
      <textarea name="note" rows="4" required placeholder="ผลการดำเนินงาน ปัญหาอุปสรรค ฯลฯ"
        class="w-full px-3 py-2.5 rounded-xl border border-line bg-plane text-sm focus:outline-none focus:ring-2 focus:ring-brand/40"></textarea>
      <div class="grid grid-cols-2 gap-3">
        <div>
          <label class="block text-xs text-ink-2 mb-1">ความคืบหน้างาน (%)</label>
          <input type="number" name="progress" min="0" max="100" step="5" value="{{ task['progress'] }}"
                 class="w-full px-3 py-2 rounded-lg border border-line bg-plane text-sm tnum">
        </div>
        <div>
          <label class="block text-xs text-ink-2 mb-1">สถานะงาน</label>
          <select name="status" class="w-full px-3 py-2 rounded-lg border border-line bg-white text-sm">
            {% for s in STATUSES %}<option value="{{ s }}" {{ 'selected' if task['status']==s }}>{{ s }}</option>{% endfor %}
          </select>
        </div>
      </div>
      <div class="grid grid-cols-2 gap-2">
        <select name="kind" class="px-3 py-2 rounded-lg border border-line bg-white text-sm">
          {% for k in KINDS %}<option value="{{ k }}">{{ k }}</option>{% endfor %}
        </select>
        <input name="doc_no" placeholder="เลขที่หนังสือ" class="px-3 py-2 rounded-lg border border-line bg-plane text-sm">
      </div>
      <input type="file" name="files" multiple
             class="w-full text-sm file:mr-3 file:px-3 file:py-1.5 file:rounded-lg file:border-0 file:bg-plane file:text-ink-2 file:font-medium file:cursor-pointer border border-line rounded-lg bg-plane p-1.5">
      <input name="reporter" placeholder="ชื่อผู้รายงาน" class="w-full px-3 py-2 rounded-lg border border-line bg-plane text-sm">
      <button class="w-full px-4 py-2.5 rounded-xl bg-brand text-white text-sm font-medium hover:bg-brand-dark">บันทึกรายงาน</button>
    </form>
  </section>

  <section id="files" class="bg-white rounded-2xl border border-line p-5">
    <div class="flex items-center mb-3">
      <h2 class="font-semibold">แนบเอกสารระดับงาน</h2>
      <span class="ml-auto text-xs text-ink-3">ทั้งงานมี {{ files|length }} รายการ</span>
    </div>
    {{ upload_form(task['id'], none, false) }}

    {% set gfiles = files|rejectattr('step_id')|list %}
    {% if gfiles %}
    <div class="mt-4 pt-3 border-t border-line divide-y divide-line/70">
      <div class="text-xs text-ink-3 pb-1">เอกสารที่ไม่ผูกกับขั้นตอน ({{ gfiles|length }})</div>
      {% for f in gfiles %}{{ file_item(f, here + '#files') }}{% endfor %}
    </div>
    {% endif %}

    <a href="{{ url_for('file_list', task_id=task['id']) }}"
       class="mt-4 block text-center text-xs text-brand hover:underline">ดูเอกสารทั้งหมดของงานนี้ →</a>
  </section>
</div>
</div>
{% endblock %}
"""

FILES = """
{% extends "base.html" %}{% from "macros.html" import kind_badge %}
{% block title %}เอกสารแนบ{% endblock %}
{% block content %}
<div class="flex items-end justify-between mb-4">
  <div>
    <h1 class="text-2xl font-bold">เอกสารแนบทั้งหมด</h1>
    <p class="text-sm text-ink-2 mt-0.5">
      หนังสือเข้า {{ counts['หนังสือเข้า'] }} · หนังสือออก {{ counts['หนังสือออก'] }} · เอกสารประกอบ {{ counts['เอกสารประกอบ'] }}
      — เอกสารทุกฉบับผูกกับงานและขั้นตอนที่เกี่ยวข้อง
    </p>
  </div>
</div>

<div class="flex flex-wrap items-center gap-2 mb-4">
  <div class="inline-flex max-w-full overflow-x-auto rounded-xl border border-line bg-white p-1">
    <a href="{{ url_for('file_list', q=q, task_id=task_id) }}"
       class="px-4 py-1.5 rounded-lg text-sm {{ 'bg-brand text-white font-medium' if not kind else 'text-ink-2 hover:bg-plane' }}">ทั้งหมด</a>
    {% for k in KINDS %}
    <a href="{{ url_for('file_list', kind=k, q=q, task_id=task_id) }}"
       class="px-4 py-1.5 rounded-lg text-sm whitespace-nowrap {{ 'bg-brand text-white font-medium' if kind==k else 'text-ink-2 hover:bg-plane' }}">{{ k }}</a>
    {% endfor %}
  </div>
  <form method="get" class="flex items-center gap-2 w-full sm:w-auto sm:ml-auto">
    {% if kind %}<input type="hidden" name="kind" value="{{ kind }}">{% endif %}
    {% if task_id %}<input type="hidden" name="task_id" value="{{ task_id }}">{% endif %}
    <input name="q" value="{{ q }}" placeholder="ค้นหาเรื่อง / เลขที่ / ชื่อไฟล์ / ชื่องาน"
           class="flex-1 sm:w-72 px-3 py-2 rounded-lg border border-line bg-white text-sm focus:outline-none focus:ring-2 focus:ring-brand/40">
    <button class="px-4 py-2 rounded-lg bg-white border border-line text-sm hover:bg-plane">ค้นหา</button>
    {% if q or kind or task_id %}<a href="{{ url_for('file_list') }}" class="px-3 py-2 text-sm text-ink-2 hover:bg-plane rounded-lg">ล้าง</a>{% endif %}
  </form>
</div>

<div class="bg-white rounded-2xl border border-line overflow-hidden">
  <div class="overflow-x-auto">
  <table class="w-full text-sm">
    <thead class="bg-plane text-ink-2 text-left">
      <tr>
        <th class="px-4 py-3 font-medium">ประเภท</th>
        <th class="px-4 py-3 font-medium">เรื่อง / ไฟล์</th>
        <th class="px-4 py-3 font-medium">งาน › ขั้นตอน</th>
        <th class="px-4 py-3 font-medium whitespace-nowrap">เลขที่ / ลงวันที่</th>
        <th class="px-4 py-3 font-medium">จาก / ถึง</th>
        <th class="px-4 py-3 font-medium whitespace-nowrap">แนบเมื่อ</th>
        <th class="px-4 py-3 font-medium w-12"></th>
      </tr>
    </thead>
    <tbody class="divide-y divide-line">
      {% for f in files %}
      <tr class="hover:bg-plane align-top">
        <td class="px-4 py-3">{{ kind_badge(f['kind']) }}</td>
        <td class="px-4 py-3 max-w-sm">
          <div class="font-medium">{{ f['subject'] or f['filename'] or '-' }}</div>
          {% if f['stored_name'] %}
          <a href="{{ url_for('file_download', file_id=f['id']) }}" class="text-xs text-brand hover:underline">
            ↓ {{ f['filename'] }} ({{ f['filesize']|filesize }})</a>
          {% else %}<div class="text-xs text-ink-3">ยังไม่มีไฟล์แนบ</div>{% endif %}
          {% if f['note'] %}<div class="text-xs text-ink-3 italic">{{ f['note'] }}</div>{% endif %}
        </td>
        <td class="px-4 py-3">
          <a href="{{ url_for('task_detail', task_id=f['task_id']) }}" class="hover:text-brand">{{ f['task_title'] }}</a>
          {% if f['step_name'] %}<div class="text-xs text-ink-3">ขั้นตอน {{ f['step_seq'] }}: {{ f['step_name'] }}</div>{% endif %}
        </td>
        <td class="px-4 py-3 tnum whitespace-nowrap">
          {{ f['doc_no'] or '-' }}<div class="text-xs text-ink-3">{{ f['doc_date']|thdate }}</div>
        </td>
        <td class="px-4 py-3">{{ f['counterparty'] or '-' }}</td>
        <td class="px-4 py-3 text-xs text-ink-3 tnum whitespace-nowrap">
          {{ f['created_at']|thdatetime }}{% if f['uploader'] %}<div>{{ f['uploader'] }}</div>{% endif %}
        </td>
        <td class="px-4 py-3">
          <form method="post" action="{{ url_for('file_delete', file_id=f['id']) }}" onsubmit="return confirm('ลบเอกสารนี้?')">
            <input type="hidden" name="next" value="{{ request.full_path }}">
            <button class="text-xs text-ink-3 hover:text-critical px-2 py-1 rounded hover:bg-red-50">ลบ</button>
          </form>
        </td>
      </tr>
      {% else %}
      <tr><td colspan="7" class="px-4 py-12 text-center text-ink-3">ไม่พบเอกสารตามเงื่อนไข</td></tr>
      {% endfor %}
    </tbody>
  </table>
  </div>
</div>
{% endblock %}
"""

REPORTS = """
{% extends "base.html" %}{% from "macros.html" import kpi, bar_row, status_badge, priority_badge, due_cell, progress_bar %}
{% block title %}สรุปรายงาน{% endblock %}
{% block content %}
<div class="flex flex-wrap items-end justify-between gap-3 mb-4">
  <div>
    <h1 class="text-2xl font-bold">สรุปรายงาน</h1>
    <p class="text-sm text-ink-2 mt-0.5">สรุปผลการติดตามงาน ขั้นตอน และเอกสารแนบ</p>
  </div>
  <div class="flex flex-wrap gap-2">
    <a href="{{ url_for('export_csv', kind='tasks') }}" class="px-4 py-2 rounded-xl border border-line bg-white text-sm hover:bg-plane">↓ งาน (CSV)</a>
    <a href="{{ url_for('export_csv', kind='steps') }}" class="px-4 py-2 rounded-xl border border-line bg-white text-sm hover:bg-plane">↓ ขั้นตอน (CSV)</a>
    <a href="{{ url_for('export_csv', kind='files') }}" class="px-4 py-2 rounded-xl border border-line bg-white text-sm hover:bg-plane">↓ เอกสาร (CSV)</a>
  </div>
</div>

<form method="get" class="bg-white rounded-2xl border border-line p-3 mb-4 flex flex-wrap items-center gap-2 text-sm">
  <span class="text-ink-2 px-1">ช่วงวันที่สร้างงาน</span>
  <input type="date" name="start" value="{{ start }}" class="px-3 py-2 rounded-lg border border-line bg-plane tnum">
  <span class="text-ink-3">ถึง</span>
  <input type="date" name="end" value="{{ end }}" class="px-3 py-2 rounded-lg border border-line bg-plane tnum">
  <button class="px-4 py-2 rounded-lg bg-brand text-white font-medium hover:bg-brand-dark">กรอง</button>
  <a href="{{ url_for('reports') }}" class="px-3 py-2 rounded-lg text-ink-2 hover:bg-plane">ล้าง</a>
</form>

<div class="grid grid-cols-2 lg:grid-cols-5 gap-3 mb-5">
  {{ kpi('งานในช่วงที่เลือก', total, 'รายการ', none) }}
  {{ kpi('เสร็จสิ้นแล้ว', done, 'รายการ', '#0ca30c') }}
  {{ kpi('อัตราความสำเร็จ', '%s%%'|format(rate), 'จากงานทั้งหมด', '#2a78d6') }}
  {{ kpi('ขั้นตอนเกินกำหนด', step_overdue|length, 'ขั้นตอน', '#d03b3b') }}
  {{ kpi('เอกสารแนบ', n_files, '%s รายการมีไฟล์'|format(with_file), none) }}
</div>

<div class="grid lg:grid-cols-3 gap-4 mb-4">
  <section class="bg-white rounded-2xl border border-line p-5">
    <h2 class="font-semibold mb-1">งานตามสถานะ</h2>
    <p class="text-xs text-ink-3 mb-4">หน่วย: รายการ</p>
    <div class="space-y-2.5">
      {% set cmap = {'รอดำเนินการ':'#898781','กำลังดำเนินการ':'#2a78d6','รอตรวจสอบ':'#fab219','เสร็จสิ้น':'#0ca30c','ยกเลิก':'#c3c2b7'} %}
      {% for s in STATUSES %}{{ bar_row(s, by_status[s], total, cmap[s]) }}{% endfor %}
    </div>
  </section>
  <section class="bg-white rounded-2xl border border-line p-5">
    <h2 class="font-semibold mb-1">ขั้นตอนทั้งระบบ</h2>
    <p class="text-xs text-ink-3 mb-4">หน่วย: ขั้นตอน</p>
    {% set stotal = steps_by_status.values()|sum %}
    <div class="space-y-2.5">
      {% set smap = {'รอดำเนินการ':'#898781','กำลังดำเนินการ':'#2a78d6','เสร็จสิ้น':'#0ca30c'} %}
      {% for s in STEP_STATUSES %}{{ bar_row(s, steps_by_status[s], stotal, smap[s]) }}{% endfor %}
    </div>
  </section>
  <section class="bg-white rounded-2xl border border-line p-5">
    <h2 class="font-semibold mb-1">เอกสารตามประเภท</h2>
    <p class="text-xs text-ink-3 mb-4">หน่วย: รายการ</p>
    {% set ktotal = by_kind.values()|sum %}
    <div class="space-y-2.5">
      {% set kmap = {'หนังสือเข้า':'#2a78d6','หนังสือออก':'#4a3aa7','เอกสารประกอบ':'#898781'} %}
      {% for k in KINDS %}{{ bar_row(k, by_kind[k], ktotal, kmap[k]) }}{% endfor %}
    </div>
  </section>
</div>

<section class="bg-white rounded-2xl border border-line overflow-hidden mb-4">
  <div class="px-5 py-3.5 border-b border-line"><h2 class="font-semibold">สรุปตามผู้รับผิดชอบ</h2></div>
  <div class="overflow-x-auto">
  <table class="w-full text-sm">
    <thead class="bg-plane text-ink-2 text-left">
      <tr><th class="px-5 py-3 font-medium">ผู้รับผิดชอบ</th>
          <th class="px-5 py-3 font-medium text-right">งานทั้งหมด</th>
          <th class="px-5 py-3 font-medium text-right">เสร็จสิ้น</th>
          <th class="px-5 py-3 font-medium text-right">เกินกำหนด</th>
          <th class="px-5 py-3 font-medium w-48">ความคืบหน้าเฉลี่ย</th></tr>
    </thead>
    <tbody class="divide-y divide-line">
      {% for r in by_assignee %}
      <tr class="hover:bg-plane">
        <td class="px-5 py-3 font-medium whitespace-nowrap">{{ r['k'] }}</td>
        <td class="px-5 py-3 text-right tnum">{{ r['c'] }}</td>
        <td class="px-5 py-3 text-right tnum text-good font-medium">{{ r['done'] }}</td>
        <td class="px-5 py-3 text-right tnum {{ 'text-critical font-medium' if r['overdue'] else 'text-ink-3' }}">{{ r['overdue'] }}</td>
        <td class="px-5 py-3">{{ progress_bar(r['avg_progress']|round|int, '#2a78d6') }}</td>
      </tr>
      {% else %}<tr><td colspan="5" class="px-5 py-10 text-center text-ink-3">ไม่มีข้อมูล</td></tr>{% endfor %}
    </tbody>
  </table>
  </div>
</section>

<section class="bg-white rounded-2xl border border-line overflow-hidden mb-4">
  <div class="px-5 py-3.5 border-b border-line flex items-center gap-2">
    <span class="w-2 h-2 rounded-full bg-critical"></span>
    <h2 class="font-semibold">ขั้นตอนที่เกินกำหนด</h2>
    <span class="text-xs text-ink-3">{{ step_overdue|length }} ขั้นตอน</span>
  </div>
  <div class="overflow-x-auto">
  <table class="w-full text-sm">
    <thead class="bg-plane text-ink-2 text-left">
      <tr><th class="px-5 py-3 font-medium">งาน</th>
          <th class="px-5 py-3 font-medium">ขั้นตอน</th>
          <th class="px-5 py-3 font-medium">ผู้รับผิดชอบ</th>
          <th class="px-5 py-3 font-medium">สถานะ</th>
          <th class="px-5 py-3 font-medium">กำหนด</th></tr>
    </thead>
    <tbody class="divide-y divide-line">
      {% for s in step_overdue %}
      <tr class="hover:bg-plane">
        <td class="px-5 py-3"><a href="{{ url_for('task_detail', task_id=s['task_id']) }}#step-{{ s['id'] }}" class="hover:text-brand">{{ s['task_title'] }}</a></td>
        <td class="px-5 py-3 font-medium">{{ s['seq'] }}. {{ s['name'] }}</td>
        <td class="px-5 py-3 whitespace-nowrap">{{ s['owner'] or s['assignee'] or '-' }}</td>
        <td class="px-5 py-3">{{ status_badge(s['status']) }}</td>
        <td class="px-5 py-3">{{ due_cell(s['due_date'], s['status']) }}</td>
      </tr>
      {% else %}<tr><td colspan="5" class="px-5 py-10 text-center text-ink-3">ไม่มีขั้นตอนที่เกินกำหนด</td></tr>{% endfor %}
    </tbody>
  </table>
  </div>
</section>

<div class="grid lg:grid-cols-2 gap-4 mb-4">
  <section class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-5 py-3.5 border-b border-line"><h2 class="font-semibold">สรุปตามประเภทงาน</h2></div>
    <table class="w-full text-sm">
      <thead class="bg-plane text-ink-2 text-left">
        <tr><th class="px-5 py-3 font-medium">ประเภท</th>
            <th class="px-5 py-3 font-medium text-right">ทั้งหมด</th>
            <th class="px-5 py-3 font-medium text-right">เสร็จสิ้น</th>
            <th class="px-5 py-3 font-medium text-right">สำเร็จ (%)</th></tr>
      </thead>
      <tbody class="divide-y divide-line">
        {% for r in by_category %}
        <tr class="hover:bg-plane">
          <td class="px-5 py-3">{{ r['k'] }}</td>
          <td class="px-5 py-3 text-right tnum">{{ r['c'] }}</td>
          <td class="px-5 py-3 text-right tnum">{{ r['done'] }}</td>
          <td class="px-5 py-3 text-right tnum font-medium">{{ (r['done'] * 100 / r['c'])|round|int }}%</td>
        </tr>
        {% else %}<tr><td colspan="4" class="px-5 py-10 text-center text-ink-3">ไม่มีข้อมูล</td></tr>{% endfor %}
      </tbody>
    </table>
  </section>

  <section class="bg-white rounded-2xl border border-line overflow-hidden">
    <div class="px-5 py-3.5 border-b border-line"><h2 class="font-semibold">เอกสารแนบรายเดือน</h2></div>
    <table class="w-full text-sm">
      <thead class="bg-plane text-ink-2 text-left">
        <tr><th class="px-5 py-3 font-medium">เดือน</th>
            <th class="px-5 py-3 font-medium text-right">หนังสือเข้า</th>
            <th class="px-5 py-3 font-medium text-right">หนังสือออก</th>
            <th class="px-5 py-3 font-medium text-right">เอกสารประกอบ</th></tr>
      </thead>
      <tbody class="divide-y divide-line">
        {% for r in file_month %}
        <tr class="hover:bg-plane">
          <td class="px-5 py-3">{% if r['m'] %}{{ months[r['m'][5:7]|int] }} {{ (r['m'][0:4]|int + 543) }}{% else %}ไม่ระบุ{% endif %}</td>
          <td class="px-5 py-3 text-right tnum">{{ r['cin'] }}</td>
          <td class="px-5 py-3 text-right tnum">{{ r['cout'] }}</td>
          <td class="px-5 py-3 text-right tnum">{{ r['cdoc'] }}</td>
        </tr>
        {% else %}<tr><td colspan="4" class="px-5 py-10 text-center text-ink-3">ไม่มีข้อมูล</td></tr>{% endfor %}
      </tbody>
    </table>
  </section>
</div>

<section class="bg-white rounded-2xl border border-line overflow-hidden">
  <div class="px-5 py-3.5 border-b border-line flex items-center gap-2">
    <span class="w-2 h-2 rounded-full bg-critical"></span>
    <h2 class="font-semibold">งานเกินกำหนด</h2>
    <span class="text-xs text-ink-3">{{ overdue|length }} รายการ</span>
  </div>
  <div class="overflow-x-auto">
  <table class="w-full text-sm">
    <thead class="bg-plane text-ink-2 text-left">
      <tr><th class="px-5 py-3 font-medium">ชื่องาน</th>
          <th class="px-5 py-3 font-medium">ผู้รับผิดชอบ</th>
          <th class="px-5 py-3 font-medium">ความสำคัญ</th>
          <th class="px-5 py-3 font-medium">สถานะ</th>
          <th class="px-5 py-3 font-medium">กำหนดส่ง</th></tr>
    </thead>
    <tbody class="divide-y divide-line">
      {% for t in overdue %}
      <tr class="hover:bg-plane">
        <td class="px-5 py-3"><a href="{{ url_for('task_detail', task_id=t['id']) }}" class="font-medium hover:text-brand">{{ t['title'] }}</a></td>
        <td class="px-5 py-3 whitespace-nowrap">{{ t['assignee'] or '-' }}</td>
        <td class="px-5 py-3">{{ priority_badge(t['priority']) }}</td>
        <td class="px-5 py-3">{{ status_badge(t['status']) }}</td>
        <td class="px-5 py-3">{{ due_cell(t['due_date'], t['status']) }}</td>
      </tr>
      {% else %}<tr><td colspan="5" class="px-5 py-10 text-center text-ink-3">ไม่มีงานเกินกำหนด</td></tr>{% endfor %}
    </tbody>
  </table>
  </div>
</section>
{% endblock %}
"""

app.jinja_loader = DictLoader({
    "base.html": BASE,
    "macros.html": MACROS,
    "dashboard.html": DASHBOARD,
    "tasks.html": TASKS,
    "task_form.html": TASK_FORM,
    "task_detail.html": TASK_DETAIL,
    "files.html": FILES,
    "reports.html": REPORTS,
})


@app.context_processor
def inject_globals():
    d = date.today()
    return {"now_display": "%d %s %d" % (d.day, THAI_MONTHS_FULL[d.month], d.year + 543)}


# ---------------------------------------------------------------------------
def cprint(text):
    """พิมพ์ข้อความออกคอนโซลโดยไม่พังเมื่อ code page ของ Windows เข้ารหัสอักขระบางตัวไม่ได้"""
    enc = (sys.stdout.encoding or "utf-8")
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode(enc, "replace").decode(enc, "replace"))


if __name__ == "__main__":
    init_db()
    cprint("=" * 64)
    cprint(" ระบบติดตามงาน : งาน > ขั้นตอน > เอกสารแนบ (แนบได้หลายไฟล์ หลายครั้ง)")
    cprint(" ฐานข้อมูล : %s" % DB_PATH)
    cprint(" ไฟล์แนบ   : %s" % UPLOAD_DIR)
    cprint(" เปิดใช้งาน: http://127.0.0.1:5000")
    cprint("=" * 64)
    app.run(debug="--debug" in sys.argv, host="127.0.0.1", port=5000)
