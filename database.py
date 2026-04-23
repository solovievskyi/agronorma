"""SQLite-шар для бота перевізників. v3."""
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

    # ──────────── Init + migrations ────────────

    def init(self):
        """Створює таблиці та виконує міграції для старих БД."""
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
                    winner_proposal_id INTEGER,
                    photo_file_id TEXT,
                    auto_close_at TIMESTAMP,
                    reminder_sent INTEGER NOT NULL DEFAULT 0,
                    created_by INTEGER,
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
                    full_name TEXT,
                    phone TEXT,
                    edrpou TEXT,
                    vehicle_tonnage REAL,
                    vehicle_type TEXT,
                    is_registered INTEGER NOT NULL DEFAULT 0,
                    is_pending INTEGER NOT NULL DEFAULT 0,
                    is_blacklisted INTEGER NOT NULL DEFAULT 0,
                    blacklist_reason TEXT,
                    wins_count INTEGER NOT NULL DEFAULT 0,
                    total_proposals INTEGER NOT NULL DEFAULT 0,
                    submitted_at TIMESTAMP,
                    registered_at TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    text TEXT,
                    sent_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            # Міграції для старих БД
            self._migrate(c, "offers", "winner_proposal_id", "INTEGER")
            self._migrate(c, "offers", "photo_file_id", "TEXT")
            self._migrate(c, "offers", "auto_close_at", "TIMESTAMP")
            self._migrate(c, "offers", "reminder_sent", "INTEGER NOT NULL DEFAULT 0")
            self._migrate(c, "offers", "created_by", "INTEGER")
            self._migrate(c, "users", "full_name", "TEXT")
            self._migrate(c, "users", "edrpou", "TEXT")
            self._migrate(c, "users", "vehicle_tonnage", "REAL")
            self._migrate(c, "users", "vehicle_type", "TEXT")
            self._migrate(
                c, "users", "is_registered", "INTEGER NOT NULL DEFAULT 0"
            )
            self._migrate(
                c, "users", "is_blacklisted", "INTEGER NOT NULL DEFAULT 0"
            )
            self._migrate(c, "users", "blacklist_reason", "TEXT")
            self._migrate(
                c, "users", "wins_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._migrate(
                c, "users", "total_proposals", "INTEGER NOT NULL DEFAULT 0"
            )
            self._migrate(c, "users", "registered_at", "TIMESTAMP")
            self._migrate(
                c, "users", "is_pending", "INTEGER NOT NULL DEFAULT 0"
            )
            self._migrate(c, "users", "submitted_at", "TIMESTAMP")

    def _migrate(self, c, table: str, column: str, type_def: str):
        """Додає колонку, якщо її ще нема."""
        cols = {r["name"] for r in c.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}")

    # ──────────── Offers ────────────

    def create_offer(self, **fields) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO offers
                   (route_from, route_to, cargo, weight_t, load_date,
                    extra_info, contact_name, contact_phone,
                    photo_file_id, auto_close_at, created_by)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    fields["route_from"],
                    fields["route_to"],
                    fields["cargo"],
                    fields["weight_t"],
                    fields.get("load_date", ""),
                    fields.get("extra_info", ""),
                    fields.get("contact_name", ""),
                    fields.get("contact_phone", ""),
                    fields.get("photo_file_id"),
                    fields.get("auto_close_at"),
                    fields.get("created_by"),
                ),
            )
            return cur.lastrowid

    def update_offer_fields(self, offer_id: int, **fields):
        """Оновити довільні поля оголошення (для редагування)."""
        if not fields:
            return
        allowed = {
            "route_from", "route_to", "cargo", "weight_t", "load_date",
            "extra_info", "contact_name", "contact_phone",
            "photo_file_id", "auto_close_at",
        }
        set_parts = []
        params = []
        for k, v in fields.items():
            if k in allowed:
                set_parts.append(f"{k}=?")
                params.append(v)
        if not set_parts:
            return
        params.append(offer_id)
        with self._conn() as c:
            c.execute(
                f"UPDATE offers SET {', '.join(set_parts)} WHERE id=?",
                tuple(params),
            )

    def set_offer_message_id(self, offer_id: int, message_id: int):
        with self._conn() as c:
            c.execute(
                "UPDATE offers SET channel_message_id=? WHERE id=?",
                (message_id, offer_id),
            )

    def set_offer_status(self, offer_id: int, status: str):
        with self._conn() as c:
            c.execute("UPDATE offers SET status=? WHERE id=?", (status, offer_id))

    def set_offer_winner(
        self, offer_id: int, proposal_id: Optional[int]
    ):
        with self._conn() as c:
            c.execute(
                "UPDATE offers SET winner_proposal_id=? WHERE id=?",
                (proposal_id, offer_id),
            )

    def mark_reminder_sent(self, offer_id: int):
        with self._conn() as c:
            c.execute(
                "UPDATE offers SET reminder_sent=1 WHERE id=?", (offer_id,)
            )

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

    def offers_pending_auto_close(self) -> list:
        """Оголошення, у яких auto_close_at уже минув і вони ще відкриті."""
        with self._conn() as c:
            rows = c.execute(
                """SELECT * FROM offers
                   WHERE status IN ('open','in_progress')
                     AND auto_close_at IS NOT NULL
                     AND auto_close_at <= CURRENT_TIMESTAMP"""
            ).fetchall()
            return [dict(r) for r in rows]

    def offers_needing_reminder(self, hours_before: int = 2) -> list:
        """Оголошення, у яких до автозакриття <= X годин і reminder_sent=0."""
        with self._conn() as c:
            rows = c.execute(
                f"""SELECT * FROM offers
                   WHERE status IN ('open','in_progress')
                     AND auto_close_at IS NOT NULL
                     AND reminder_sent=0
                     AND auto_close_at <= datetime('now', '+{hours_before} hours')
                     AND auto_close_at > CURRENT_TIMESTAMP"""
            ).fetchall()
            return [dict(r) for r in rows]

    # ──────────── Proposals ────────────

    def get_or_create_proposal(
        self, offer_id: int, user_id: int, username: str, first_name: str
    ):
        """Повертає (request_id, proposal_dict)."""
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM proposals WHERE offer_id=? AND user_id=?",
                (offer_id, user_id),
            ).fetchone()
            if row:
                c.execute(
                    "UPDATE proposals SET username=?, first_name=? WHERE id=?",
                    (username, first_name, row["id"]),
                )
                row = c.execute(
                    "SELECT * FROM proposals WHERE id=?", (row["id"],)
                ).fetchone()
                return row["id"], dict(row)

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
            c.execute(
                "UPDATE users SET total_proposals = total_proposals + 1 "
                "WHERE user_id=?",
                (user_id,),
            )
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

    def user_has_proposal(self, offer_id: int, user_id: int) -> bool:
        with self._conn() as c:
            row = c.execute(
                "SELECT 1 FROM proposals WHERE offer_id=? AND user_id=?",
                (offer_id, user_id),
            ).fetchone()
            return row is not None

    def users_without_proposal_for(self, offer_id: int) -> list:
        """Зареєстровані, не-заблоковані, які ще не подали ціну."""
        with self._conn() as c:
            rows = c.execute(
                """SELECT u.* FROM users u
                   WHERE u.is_registered=1 AND u.is_blacklisted=0
                     AND u.user_id NOT IN
                       (SELECT user_id FROM proposals WHERE offer_id=?)""",
                (offer_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ──────────── Users / carriers ────────────

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

    def submit_registration(
        self,
        user_id: int,
        full_name: str,
        phone: str,
        edrpou: Optional[str],
    ):
        """Перевізник подав заявку на реєстрацію. Потрібне схвалення адміна."""
        with self._conn() as c:
            c.execute(
                """UPDATE users SET
                       full_name=?, phone=?, edrpou=?,
                       is_pending=1,
                       is_registered=0,
                       submitted_at=CURRENT_TIMESTAMP,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE user_id=?""",
                (full_name, phone, edrpou, user_id),
            )

    def approve_user(self, user_id: int):
        """Адмін схвалив — користувач отримує доступ."""
        with self._conn() as c:
            c.execute(
                """UPDATE users SET
                       is_pending=0,
                       is_registered=1,
                       is_blacklisted=0,
                       blacklist_reason=NULL,
                       registered_at=CURRENT_TIMESTAMP,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE user_id=?""",
                (user_id,),
            )
            # Підтягнути phone у пропозиції, де його ще нема
            c.execute(
                """UPDATE proposals
                   SET phone=(SELECT phone FROM users WHERE user_id=?)
                   WHERE user_id=? AND (phone IS NULL OR phone='')""",
                (user_id, user_id),
            )

    def reject_user(self, user_id: int, reason: str = ""):
        """Адмін відхилив — користувач не може подавати пропозиції.
        Блокується, щоб не створював нові заявки на реєстрацію."""
        full_reason = "Заявку на реєстрацію відхилено"
        if reason:
            full_reason += f": {reason}"
        with self._conn() as c:
            c.execute(
                """UPDATE users SET
                       is_pending=0,
                       is_registered=0,
                       is_blacklisted=1,
                       blacklist_reason=?,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE user_id=?""",
                (full_reason, user_id),
            )

    def is_pending(self, user_id: int) -> bool:
        u = self.get_user(user_id)
        return bool(u and u.get("is_pending"))

    def list_pending_users(self) -> list:
        with self._conn() as c:
            rows = c.execute(
                """SELECT * FROM users WHERE is_pending=1
                   ORDER BY submitted_at"""
            ).fetchall()
            return [dict(r) for r in rows]

    def update_user_phone(self, user_id: int, phone: str):
        with self._conn() as c:
            c.execute(
                """INSERT INTO users (user_id, phone) VALUES (?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       phone=excluded.phone,
                       updated_at=CURRENT_TIMESTAMP""",
                (user_id, phone),
            )
            c.execute(
                "UPDATE proposals SET phone=? WHERE user_id=? AND "
                "(phone IS NULL OR phone='')",
                (phone, user_id),
            )

    def get_user(self, user_id: int) -> Optional[dict]:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def is_registered(self, user_id: int) -> bool:
        u = self.get_user(user_id)
        return bool(u and u.get("is_registered"))

    def is_blacklisted(self, user_id: int) -> bool:
        u = self.get_user(user_id)
        return bool(u and u.get("is_blacklisted"))

    def set_blacklist(
        self, user_id: int, value: bool, reason: str = ""
    ):
        with self._conn() as c:
            c.execute(
                """UPDATE users SET is_blacklisted=?, blacklist_reason=?
                   WHERE user_id=?""",
                (1 if value else 0, reason if value else None, user_id),
            )

    def increment_wins(self, user_id: int):
        with self._conn() as c:
            c.execute(
                "UPDATE users SET wins_count = wins_count + 1 WHERE user_id=?",
                (user_id,),
            )

    def list_registered_users(self) -> list:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM users WHERE is_registered=1 AND is_blacklisted=0"
            ).fetchall()
            return [dict(r) for r in rows]

    def list_all_users_for_broadcast(self) -> list:
        """Усі, хто хоч раз запускав бота — для broadcast."""
        with self._conn() as c:
            rows = c.execute(
                "SELECT user_id FROM users WHERE is_blacklisted=0"
            ).fetchall()
            return [r["user_id"] for r in rows]

    # ──────────── Admins ────────────

    def add_admin(
        self, user_id: int, username: str = "", first_name: str = ""
    ):
        with self._conn() as c:
            c.execute(
                """INSERT INTO admins (user_id, username, first_name)
                   VALUES (?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       username=excluded.username,
                       first_name=excluded.first_name""",
                (user_id, username, first_name),
            )

    def remove_admin(self, user_id: int) -> bool:
        with self._conn() as c:
            cur = c.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
            return cur.rowcount > 0

    def is_admin_db(self, user_id: int) -> bool:
        with self._conn() as c:
            row = c.execute(
                "SELECT 1 FROM admins WHERE user_id=?", (user_id,)
            ).fetchone()
            return row is not None

    def list_admins(self) -> list:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM admins ORDER BY added_at"
            ).fetchall()
            return [dict(r) for r in rows]

    # ──────────── Broadcasts ────────────

    def log_broadcast(
        self, admin_id: int, text: str, sent: int, failed: int
    ) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO broadcasts (admin_id, text, sent_count, failed_count)
                   VALUES (?, ?, ?, ?)""",
                (admin_id, text, sent, failed),
            )
            return cur.lastrowid

    # ──────────── Stats ────────────

    def stats_summary(self, days: int = 30) -> dict:
        with self._conn() as c:
            total = c.execute(
                f"""SELECT COUNT(*) AS c FROM offers
                    WHERE created_at >= datetime('now', '-{days} days')"""
            ).fetchone()["c"]
            closed = c.execute(
                f"""SELECT COUNT(*) AS c FROM offers
                    WHERE status='closed'
                      AND created_at >= datetime('now', '-{days} days')"""
            ).fetchone()["c"]
            avg_vat = c.execute(
                f"""SELECT AVG(min_price) AS a FROM (
                      SELECT MIN(p.price_with_vat) AS min_price
                      FROM proposals p
                      JOIN offers o ON o.id=p.offer_id
                      WHERE p.price_with_vat IS NOT NULL
                        AND o.created_at >= datetime('now', '-{days} days')
                      GROUP BY p.offer_id
                    )"""
            ).fetchone()["a"]
            top_routes = c.execute(
                f"""SELECT route_from || ' → ' || route_to AS route,
                           COUNT(*) AS c
                    FROM offers
                    WHERE created_at >= datetime('now', '-{days} days')
                    GROUP BY route
                    ORDER BY c DESC
                    LIMIT 5"""
            ).fetchall()
            top_winners = c.execute(
                """SELECT user_id, full_name, first_name, username, wins_count
                   FROM users
                   WHERE wins_count > 0
                   ORDER BY wins_count DESC
                   LIMIT 5"""
            ).fetchall()
            registered = c.execute(
                "SELECT COUNT(*) AS c FROM users WHERE is_registered=1"
            ).fetchone()["c"]
            return {
                "total": total,
                "closed": closed,
                "avg_vat": avg_vat,
                "top_routes": [dict(r) for r in top_routes],
                "top_winners": [dict(r) for r in top_winners],
                "registered_carriers": registered,
            }
