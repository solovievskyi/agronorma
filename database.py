"""SQLite-шар для бота перевізників."""
import sqlite3
from typing import Optional


class Database:
    def __init__(self, path: str):
        self.path = path

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init(self):
        """Створює таблиці, якщо їх ще немає."""
        with self._conn() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS offers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    route_from TEXT NOT NULL,
                    route_to TEXT NOT NULL,
                    cargo TEXT NOT NULL,
                    weight_t REAL NOT NULL,
                    load_date TEXT,
                    extra_info TEXT,
                    contact_name TEXT,
                    contact_phone TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    channel_message_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS proposals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    offer_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    phone TEXT,
                    price_with_vat REAL,
                    price_without_vat REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(offer_id, user_id),
                    FOREIGN KEY (offer_id) REFERENCES offers(id)
                );

                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    phone TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

    # ---------- Offers ----------

    def create_offer(self, **fields) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO offers
                   (route_from, route_to, cargo, weight_t, load_date,
                    extra_info, contact_name, contact_phone)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    fields["route_from"],
                    fields["route_to"],
                    fields["cargo"],
                    fields["weight_t"],
                    fields.get("load_date", ""),
                    fields.get("extra_info", ""),
                    fields.get("contact_name", ""),
                    fields.get("contact_phone", ""),
                ),
            )
            return cur.lastrowid

    def set_offer_message_id(self, offer_id: int, message_id: int):
        with self._conn() as c:
            c.execute(
                "UPDATE offers SET channel_message_id=? WHERE id=?",
                (message_id, offer_id),
            )

    def set_offer_status(self, offer_id: int, status: str):
        with self._conn() as c:
            c.execute("UPDATE offers SET status=? WHERE id=?", (status, offer_id))

    def get_offer(self, offer_id: int) -> Optional[dict]:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM offers WHERE id=?", (offer_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_offers(self, statuses=None) -> list:
        with self._conn() as c:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                rows = c.execute(
                    f"SELECT * FROM offers WHERE status IN ({placeholders}) "
                    "ORDER BY id DESC",
                    tuple(statuses),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM offers ORDER BY id DESC"
                ).fetchall()
            return [dict(r) for r in rows]

    # ---------- Proposals ----------

    def get_or_create_proposal(
        self, offer_id: int, user_id: int, username: str, first_name: str
    ):
        """Повертає (request_id, proposal_dict).

        Якщо перевізник уже відкривав цю заявку — повертається стара.
        Інакше створюється нова з унікальним request_id.
        """
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM proposals WHERE offer_id=? AND user_id=?",
                (offer_id, user_id),
            ).fetchone()
            if row:
                return row["id"], dict(row)

            # Забираємо phone із users, якщо був раніше переданий
            user_row = c.execute(
                "SELECT phone FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            phone = user_row["phone"] if user_row else None

            cur = c.execute(
                """INSERT INTO proposals
                   (offer_id, user_id, username, first_name, phone)
                   VALUES (?, ?, ?, ?, ?)""",
                (offer_id, user_id, username, first_name, phone),
            )
            rid = cur.lastrowid
            row = c.execute(
                "SELECT * FROM proposals WHERE id=?", (rid,)
            ).fetchone()
            return rid, dict(row)

    def get_proposal(self, request_id: int) -> Optional[dict]:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM proposals WHERE id=?", (request_id,)
            ).fetchone()
            return dict(row) if row else None

    def update_proposal_price(
        self,
        request_id: int,
        price_with_vat: Optional[float] = None,
        price_without_vat: Optional[float] = None,
    ):
        with self._conn() as c:
            if price_with_vat is not None:
                c.execute(
                    "UPDATE proposals SET price_with_vat=?, "
                    "updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (price_with_vat, request_id),
                )
            if price_without_vat is not None:
                c.execute(
                    "UPDATE proposals SET price_without_vat=?, "
                    "updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (price_without_vat, request_id),
                )

    def list_proposals(self, offer_id: int) -> list:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM proposals WHERE offer_id=? ORDER BY id",
                (offer_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def count_proposals(self, offer_id: int) -> int:
        with self._conn() as c:
            row = c.execute(
                "SELECT COUNT(*) AS c FROM proposals WHERE offer_id=?",
                (offer_id,),
            ).fetchone()
            return row["c"] if row else 0

    # ---------- Users ----------

    def upsert_user(self, user_id: int, username: str, first_name: str):
        with self._conn() as c:
            c.execute(
                """INSERT INTO users (user_id, username, first_name)
                   VALUES (?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       username=excluded.username,
                       first_name=excluded.first_name,
                       updated_at=CURRENT_TIMESTAMP""",
                (user_id, username, first_name),
            )

    def update_user_phone(self, user_id: int, phone: str):
        with self._conn() as c:
            c.execute(
                """INSERT INTO users (user_id, phone) VALUES (?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       phone=excluded.phone,
                       updated_at=CURRENT_TIMESTAMP""",
                (user_id, phone),
            )
            # Розповсюджуємо номер на існуючі пропозиції
            c.execute(
                "UPDATE proposals SET phone=? WHERE user_id=? AND phone IS NULL",
                (phone, user_id),
            )
