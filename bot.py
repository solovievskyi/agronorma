"""
Telegram-бот для роботи з перевізниками (канал + бот).

v2 — додано:
  - Кілька адміністраторів (у БД) + один "супер-адмін" (з env).
  - Всі команди продубльовано кнопками (persistent reply-меню
    + inline-кнопки під списком оголошень).
  - HTML-посилання на Telegram-профіль перевізника у сповіщеннях
    і звітах (клікабельне).
  - Послідовні номери оголошень/заявок гарантовано AUTOINCREMENT.
"""
import asyncio
import logging
import os
import re
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    MessageOriginUser,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from database import Database

# ────────────────────  Конфігурація  ────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPER_ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
DB_PATH = os.getenv("DB_PATH", "bot.db")

if not BOT_TOKEN or not SUPER_ADMIN_ID or not CHANNEL_ID or not BOT_USERNAME:
    raise RuntimeError(
        "Задайте змінні середовища BOT_TOKEN, ADMIN_ID, CHANNEL_ID, BOT_USERNAME"
    )

# ────────────────────  Логування  ────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ────────────────────  Ініціалізація  ────────────────────
db = Database(DB_PATH)
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


# ────────────────────  FSM-стани  ────────────────────
class NewOfferStates(StatesGroup):
    route_from = State()
    route_to = State()
    cargo = State()
    weight = State()
    load_date = State()
    extra_info = State()
    contact_name = State()
    contact_phone = State()


class PriceInputStates(StatesGroup):
    with_vat = State()
    without_vat = State()


class AddAdminStates(StatesGroup):
    waiting_for_user = State()


# ────────────────────  Тексти кнопок  ────────────────────
# Адмін:
ADMIN_BTN_NEW = "🆕 Нове оголошення"
ADMIN_BTN_LIST = "📋 Активні оголошення"
ADMIN_BTN_ADMINS = "👥 Адміни"

# Перевізник:
BTN_WITH_VAT = "📝 Ввести тариф з ПДВ"
BTN_WITHOUT_VAT = "📝 Ввести тариф без ПДВ"
BTN_CONTACT = "📱 Передати контакт"
BTN_CANCEL = "❌ Скасувати"


# ────────────────────  Права доступу  ────────────────────
def is_super_admin(user_id: int) -> bool:
    return user_id == SUPER_ADMIN_ID


def is_admin(user_id: int) -> bool:
    return is_super_admin(user_id) or db.is_admin_db(user_id)


# ────────────────────  Утиліти  ────────────────────
def html_escape(text: str) -> str:
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def user_profile_link(
    user_id: int, username: Optional[str], first_name: Optional[str]
) -> str:
    """HTML-посилання на Telegram-профіль.

    Якщо є username — посилання на https://t.me/username (працює в усіх клієнтах).
    Якщо немає — посилання tg://user?id=X (працює тільки в Telegram).
    """
    if username:
        url = f"https://t.me/{username}"
        display = f"@{html_escape(username)}"
    else:
        url = f"tg://user?id={user_id}"
        display = html_escape(first_name or f"id{user_id}")
    return f'<a href="{url}">{display}</a>'


def status_emoji(status: str) -> str:
    return {"open": "🟢", "in_progress": "🟡", "closed": "🔴"}.get(status, "⚪")


def status_text(status: str) -> str:
    return {
        "open": "відкрито",
        "in_progress": "в роботі",
        "closed": "закрито",
    }.get(status, status)


def fmt_weight(weight) -> str:
    w = float(weight)
    return str(int(w)) if w.is_integer() else str(w)


def channel_post_url(channel_message_id: int) -> str:
    if not channel_message_id:
        return ""
    cid = str(CHANNEL_ID).replace("-100", "", 1)
    return f"https://t.me/c/{cid}/{channel_message_id}"


def parse_price(text: str) -> Optional[float]:
    try:
        price = float(text.replace(",", ".").replace(" ", ""))
        if price <= 0:
            return None
        return price
    except (ValueError, AttributeError):
        return None


# ────────────────────  Форматування повідомлень  ────────────────────
def format_offer_for_channel(offer: dict) -> str:
    return (
        f"<b>Оголошення #{offer['id']}</b> "
        f"{status_emoji(offer['status'])} {status_text(offer['status'])}\n"
        f"<u>Опис перевезення:</u>\n"
        f"{html_escape(offer['route_from'])} - {html_escape(offer['route_to'])}\n"
        f"{fmt_weight(offer['weight_t'])} т\n"
        f"{html_escape(offer['cargo'])}\n"
        f"<u>Тариф грн/т вказує перевізник в боті</u>\n\n"
        f"<b>Додатково:</b>\n"
        f"{html_escape(offer['extra_info']) or '—'}\n"
        f"{html_escape(offer['contact_phone'])} "
        f"{html_escape(offer['contact_name'])}"
    )


def format_offer_for_carrier(
    offer: dict, request_id: int, proposal: Optional[dict]
) -> str:
    def price_str(price):
        if price is None:
            return "⚠ очікування на введення грн/т"
        return f"{fmt_weight(price)} грн/т"

    p_with = proposal["price_with_vat"] if proposal else None
    p_without = proposal["price_without_vat"] if proposal else None

    link = channel_post_url(offer.get("channel_message_id"))
    offer_ref = (
        f'<a href="{link}">Оголошення #{offer["id"]}</a>'
        if link
        else f'Оголошення #{offer["id"]}'
    )

    return (
        f"<b>Заявка #{request_id}</b> - {offer_ref}\n"
        f"Статус: {status_emoji(offer['status'])} "
        f"{status_text(offer['status'])}\n\n"
        f"<u>Опис перевезення:</u>\n"
        f"{html_escape(offer['route_from'])} - "
        f"{html_escape(offer['route_to'])}\n"
        f"{fmt_weight(offer['weight_t'])} т\n"
        f"{html_escape(offer['cargo'])}\n"
        f"Тариф перевізника з ПДВ: {price_str(p_with)}\n"
        f"Тариф перевізника без ПДВ: {price_str(p_without)}\n\n"
        f"<b>Додатково:</b>\n"
        f"{html_escape(offer['extra_info']) or '—'}\n"
        f"{html_escape(offer['contact_phone'])} "
        f"{html_escape(offer['contact_name'])}"
    )


# ────────────────────  Клавіатури  ────────────────────
def admin_menu_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """Постійне меню адміна (внизу чату)."""
    rows = [[KeyboardButton(text=ADMIN_BTN_NEW)]]
    if is_super_admin(user_id):
        rows.append([
            KeyboardButton(text=ADMIN_BTN_LIST),
            KeyboardButton(text=ADMIN_BTN_ADMINS),
        ])
    else:
        rows.append([KeyboardButton(text=ADMIN_BTN_LIST)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def carrier_card_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WITH_VAT)],
            [KeyboardButton(text=BTN_WITHOUT_VAT)],
            [
                KeyboardButton(text=BTN_CONTACT),
                KeyboardButton(text=BTN_CANCEL),
            ],
        ],
        resize_keyboard=True,
    )


def price_input_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
    )


def new_offer_keyboard() -> ReplyKeyboardMarkup:
    """Поки адмін створює оголошення — показуємо тільки «Скасувати»."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
    )


def channel_offer_keyboard(offer_id: int) -> InlineKeyboardMarkup:
    url = f"https://t.me/{BOT_USERNAME}?start=offer_{offer_id}"
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="💵 Ввести вашу пропозицію щодо тарифу",
                url=url,
            )
        ]]
    )


def offer_actions_inline(offer: dict) -> InlineKeyboardMarkup:
    """Inline-кнопки під елементом списку /list."""
    row1 = [InlineKeyboardButton(
        text="📊 Звіт", callback_data=f"report:{offer['id']}"
    )]
    if offer["status"] == "closed":
        row1.append(InlineKeyboardButton(
            text="🟢 Відкрити", callback_data=f"reopen:{offer['id']}"
        ))
    else:
        row1.append(InlineKeyboardButton(
            text="🔴 Закрити", callback_data=f"close:{offer['id']}"
        ))
    return InlineKeyboardMarkup(inline_keyboard=[row1])


def admins_list_inline(
    admins: list, super_id: int, is_super: bool
) -> InlineKeyboardMarkup:
    rows = []
    for a in admins:
        link_name = a["username"] and f"@{a['username']}" or (
            a["first_name"] or f"id{a['user_id']}"
        )
        if is_super and a["user_id"] != super_id:
            rows.append([InlineKeyboardButton(
                text=f"🗑 {link_name}",
                callback_data=f"rmadmin:{a['user_id']}",
            )])
    if is_super:
        rows.append([InlineKeyboardButton(
            text="➕ Додати адміна", callback_data="addadmin"
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


# ────────────────────  Робота з каналом  ────────────────────
async def update_channel_post(offer: dict):
    if not offer.get("channel_message_id"):
        return
    try:
        await bot.edit_message_text(
            format_offer_for_channel(offer),
            chat_id=CHANNEL_ID,
            message_id=offer["channel_message_id"],
            reply_markup=(
                channel_offer_keyboard(offer["id"])
                if offer["status"] != "closed"
                else None
            ),
        )
    except Exception as e:
        logger.warning("Не вдалось оновити пост у каналі: %s", e)


async def notify_admins_new_price(
    offer: dict, proposal: dict, user, kind: str, price: float
):
    profile = user_profile_link(
        user.id, user.username, user.first_name
    )
    text = (
        f"💰 <b>Нова пропозиція</b> по оголошенню #{offer['id']}\n"
        f"Перевізник: {profile}"
        f"{' 📱 ' + html_escape(proposal['phone']) if proposal.get('phone') else ''}\n"
        f"Заявка #{proposal['id']}\n"
        f"Маршрут: {html_escape(offer['route_from'])} → "
        f"{html_escape(offer['route_to'])} "
        f"({html_escape(offer['cargo'])}, {fmt_weight(offer['weight_t'])}т)\n"
        f"Тариф {kind}: <b>{fmt_weight(price)} грн/т</b>\n\n"
        f"Поточні ціни у цій заявці:\n"
        f"  з ПДВ: "
        f"{fmt_weight(proposal['price_with_vat']) + ' грн/т' if proposal['price_with_vat'] is not None else '—'}\n"
        f"  без ПДВ: "
        f"{fmt_weight(proposal['price_without_vat']) + ' грн/т' if proposal['price_without_vat'] is not None else '—'}"
    )
    # Надсилаємо всім адмінам (супер + у БД)
    admin_ids = {SUPER_ADMIN_ID}
    for a in db.list_admins():
        admin_ids.add(a["user_id"])
    for aid in admin_ids:
        try:
            await bot.send_message(aid, text, disable_web_page_preview=True)
        except Exception:
            logger.exception("Не вдалось сповістити адміна %s", aid)


# ══════════════════════  КОМАНДИ + КНОПКИ (АДМІН)  ══════════════════════

# ---- /start ----
@router.message(CommandStart(deep_link=True))
async def cmd_start_deeplink(
    message: Message, command: CommandObject, state: FSMContext
):
    arg = command.args or ""
    m = re.match(r"offer_(\d+)", arg)
    if not m:
        await cmd_start_regular(message, state)
        return
    offer_id = int(m.group(1))
    offer = db.get_offer(offer_id)
    if not offer:
        await message.answer(
            "⚠ Оголошення не знайдено. Можливо, воно вже видалене."
        )
        return

    db.upsert_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.first_name or "",
    )

    request_id, proposal = db.get_or_create_proposal(
        offer_id=offer_id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "",
    )

    await state.clear()
    await state.update_data(
        current_offer_id=offer_id, current_request_id=request_id
    )

    if offer["status"] == "closed":
        await message.answer(
            f"⚠ Оголошення #{offer_id} вже закрито. Нові пропозиції не приймаються.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await message.answer(
        format_offer_for_carrier(offer, request_id, proposal),
        reply_markup=carrier_card_keyboard(),
        disable_web_page_preview=True,
    )


@router.message(CommandStart())
async def cmd_start_regular(message: Message, state: FSMContext):
    await state.clear()
    db.upsert_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.first_name or "",
    )
    if is_admin(message.from_user.id):
        tag = "Супер-адмін" if is_super_admin(message.from_user.id) else "Адмін"
        await message.answer(
            f"👋 <b>Панель адміністратора</b> ({tag})\n\n"
            f"Використовуйте кнопки внизу екрана.\n"
            f"Ваш Telegram ID: <code>{message.from_user.id}</code>",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )
    else:
        await message.answer(
            "👋 Вітаємо! Цей бот приймає ваші пропозиції щодо тарифів "
            "на перевезення.\n\n"
            "Щоб надіслати пропозицію — натисніть кнопку "
            "<b>«Ввести вашу пропозицію щодо тарифу»</b> під оголошенням "
            "у каналі.\n\n"
            f"Ваш Telegram ID: <code>{message.from_user.id}</code>",
            reply_markup=ReplyKeyboardRemove(),
        )


# ---- /new і кнопка «Нове оголошення» ----
async def _start_new_offer(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Ця команда доступна тільки адміністратору.")
        return
    await state.clear()
    await state.set_state(NewOfferStates.route_from)
    await message.answer(
        "🆕 <b>Створення нового оголошення</b>\n\n"
        "Введіть маршрут <b>ЗВІДКИ</b>:",
        reply_markup=new_offer_keyboard(),
    )


@router.message(Command("new"))
async def cmd_new(message: Message, state: FSMContext):
    await _start_new_offer(message, state)


@router.message(F.text == ADMIN_BTN_NEW)
async def btn_new(message: Message, state: FSMContext):
    await _start_new_offer(message, state)


# ---- /cancel і кнопка «Скасувати» — універсальна ----
async def _universal_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    data = await state.get_data()

    # Якщо був у режимі введення ціни — повертаємо картку перевізника
    if current and current.startswith("PriceInputStates"):
        keep = {k: v for k, v in data.items() if k.startswith("current_")}
        await state.clear()
        if keep:
            await state.update_data(**keep)
            await message.answer(
                "❎ Вийшли з режиму введення ціни.",
                reply_markup=carrier_card_keyboard(),
            )
        else:
            await message.answer(
                "❎ Вийшли.", reply_markup=ReplyKeyboardRemove()
            )
        return

    # Якщо був у процесі створення оголошення — скасовуємо
    if current and current.startswith("NewOfferStates"):
        await state.clear()
        await message.answer(
            "❎ Створення оголошення скасовано.",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )
        return

    # Якщо був у процесі додавання адміна — скасовуємо
    if current and current.startswith("AddAdminStates"):
        await state.clear()
        await message.answer(
            "❎ Додавання адміна скасовано.",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )
        return

    # Без активного стану — просто повертаємо головне меню
    await state.clear()
    if is_admin(message.from_user.id):
        await message.answer(
            "Головне меню.",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )
    elif data.get("current_offer_id"):
        await state.update_data(
            current_offer_id=data["current_offer_id"],
            current_request_id=data.get("current_request_id"),
        )
        await message.answer(
            "—", reply_markup=carrier_card_keyboard()
        )
    else:
        await message.answer("—", reply_markup=ReplyKeyboardRemove())


@router.message(Command("cancel"))
@router.message(Command("reset"))
async def cmd_cancel(message: Message, state: FSMContext):
    await _universal_cancel(message, state)


@router.message(F.text == BTN_CANCEL)
async def btn_cancel(message: Message, state: FSMContext):
    await _universal_cancel(message, state)


# ---- Майстер створення оголошення ----
@router.message(NewOfferStates.route_from)
async def new_route_from(message: Message, state: FSMContext):
    await state.update_data(route_from=message.text.strip())
    await state.set_state(NewOfferStates.route_to)
    await message.answer("Введіть маршрут <b>КУДИ</b>:")


@router.message(NewOfferStates.route_to)
async def new_route_to(message: Message, state: FSMContext):
    await state.update_data(route_to=message.text.strip())
    await state.set_state(NewOfferStates.cargo)
    await message.answer("Який <b>вантаж</b>? (Соя, пшениця, кукурудза…)")


@router.message(NewOfferStates.cargo)
async def new_cargo(message: Message, state: FSMContext):
    await state.update_data(cargo=message.text.strip())
    await state.set_state(NewOfferStates.weight)
    await message.answer("Вага, <b>т</b>? (число, наприклад 600)")


@router.message(NewOfferStates.weight)
async def new_weight(message: Message, state: FSMContext):
    weight = parse_price(message.text)
    if weight is None:
        await message.answer("⚠ Введіть число, наприклад: 600 або 50.5")
        return
    await state.update_data(weight_t=weight)
    await state.set_state(NewOfferStates.load_date)
    await message.answer(
        "<b>Дата завантаження?</b>\n(з понеділка / 25.04.2026 / зараз)"
    )


@router.message(NewOfferStates.load_date)
async def new_load_date(message: Message, state: FSMContext):
    await state.update_data(load_date=message.text.strip())
    await state.set_state(NewOfferStates.extra_info)
    await message.answer(
        "<b>Додаткова інформація?</b>\n"
        "(борти, умови, коментарі — або «-» якщо немає)"
    )


@router.message(NewOfferStates.extra_info)
async def new_extra(message: Message, state: FSMContext):
    txt = message.text.strip()
    await state.update_data(extra_info="" if txt == "-" else txt)
    await state.set_state(NewOfferStates.contact_name)
    await message.answer("<b>Контактна особа?</b> (ім'я)")


@router.message(NewOfferStates.contact_name)
async def new_contact_name(message: Message, state: FSMContext):
    await state.update_data(contact_name=message.text.strip())
    await state.set_state(NewOfferStates.contact_phone)
    await message.answer("<b>Контактний телефон?</b>")


@router.message(NewOfferStates.contact_phone)
async def new_contact_phone(message: Message, state: FSMContext):
    data = await state.update_data(contact_phone=message.text.strip())
    await state.clear()

    offer_id = db.create_offer(**data)
    offer = db.get_offer(offer_id)
    text = format_offer_for_channel(offer)

    try:
        sent = await bot.send_message(
            CHANNEL_ID,
            text,
            reply_markup=channel_offer_keyboard(offer_id),
        )
        db.set_offer_message_id(offer_id, sent.message_id)
        await message.answer(
            f"✅ Оголошення <b>#{offer_id}</b> опубліковано в каналі.",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )
    except Exception as e:
        logger.exception("Не вдалось опублікувати в канал")
        await message.answer(
            f"⚠ Не вдалось опублікувати в канал: <code>{html_escape(str(e))}</code>\n"
            f"Перевірте, що бот є адміністратором каналу з правом "
            f"«Публікувати повідомлення» і що CHANNEL_ID коректний.",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )


# ---- /list і кнопка «Активні оголошення» ----
async def _show_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    offers = db.list_offers(statuses=["open", "in_progress"])
    if not offers:
        await message.answer(
            "Немає активних оголошень.",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )
        return
    await message.answer(f"<b>Активних оголошень: {len(offers)}</b>")
    for o in offers:
        cnt = db.count_proposals(o["id"])
        text = (
            f"{status_emoji(o['status'])} <b>#{o['id']}</b> "
            f"{status_text(o['status'])}\n"
            f"{html_escape(o['route_from'])} → {html_escape(o['route_to'])}\n"
            f"{html_escape(o['cargo'])}, {fmt_weight(o['weight_t'])}т\n"
            f"Пропозицій: <b>{cnt}</b>"
        )
        await message.answer(text, reply_markup=offer_actions_inline(o))


@router.message(Command("list"))
async def cmd_list(message: Message):
    await _show_list(message)


@router.message(F.text == ADMIN_BTN_LIST)
async def btn_list(message: Message):
    await _show_list(message)


# ---- Звіт (інлайн-кнопка) + /report ----
async def _send_report(target, offer_id: int):
    """target — або Message, або CallbackQuery."""
    offer = db.get_offer(offer_id)
    if not offer:
        await target.answer(f"⚠ Оголошення #{offer_id} не знайдено.")
        return

    proposals = db.list_proposals(offer_id)
    if not proposals:
        await target.answer(
            f"По оголошенню #{offer_id} поки немає пропозицій."
        )
        return

    def p_str(v):
        return f"{fmt_weight(v)} грн/т" if v is not None else "—"

    def min_price(p):
        vals = [
            v for v in (p["price_with_vat"], p["price_without_vat"])
            if v is not None
        ]
        return min(vals) if vals else float("inf")

    sorted_p = sorted(proposals, key=min_price)

    lines = [
        f"📊 <b>Звіт по оголошенню #{offer_id}</b>",
        f"{html_escape(offer['route_from'])} → "
        f"{html_escape(offer['route_to'])} | "
        f"{html_escape(offer['cargo'])} {fmt_weight(offer['weight_t'])}т",
        f"Статус: {status_emoji(offer['status'])} "
        f"{status_text(offer['status'])}",
        f"Пропозицій: <b>{len(proposals)}</b>",
        "",
        "<b>Пропозиції (від найнижчої):</b>",
    ]
    for i, p in enumerate(sorted_p, 1):
        profile = user_profile_link(
            p["user_id"], p["username"], p["first_name"]
        )
        phone = f" 📱 {html_escape(p['phone'])}" if p["phone"] else ""
        lines.append(
            f"{i}. <b>Заявка #{p['id']}</b> — {profile}{phone}\n"
            f"   з ПДВ: {p_str(p['price_with_vat'])} | "
            f"без ПДВ: {p_str(p['price_without_vat'])}"
        )

    with_vat = [p for p in proposals if p["price_with_vat"] is not None]
    without_vat = [p for p in proposals if p["price_without_vat"] is not None]
    lines.append("")
    if with_vat:
        best = min(with_vat, key=lambda p: p["price_with_vat"])
        profile = user_profile_link(
            best["user_id"], best["username"], best["first_name"]
        )
        lines.append(
            f"🏆 <b>Найкраща з ПДВ:</b> "
            f"{fmt_weight(best['price_with_vat'])} грн/т "
            f"({profile}, заявка #{best['id']})"
        )
    if without_vat:
        best = min(without_vat, key=lambda p: p["price_without_vat"])
        profile = user_profile_link(
            best["user_id"], best["username"], best["first_name"]
        )
        lines.append(
            f"🏆 <b>Найкраща без ПДВ:</b> "
            f"{fmt_weight(best['price_without_vat'])} грн/т "
            f"({profile}, заявка #{best['id']})"
        )

    await target.answer("\n".join(lines), disable_web_page_preview=True)


@router.message(Command("report"))
async def cmd_report(message: Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer(
            "Використання: <code>/report &lt;номер&gt;</code>\n"
            "Або натисніть «📋 Активні оголошення» і виберіть «📊 Звіт»."
        )
        return
    try:
        offer_id = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    await _send_report(message, offer_id)


@router.callback_query(F.data.startswith("report:"))
async def cb_report(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Немає прав.", show_alert=True)
        return
    offer_id = int(cb.data.split(":")[1])
    await _send_report(cb.message, offer_id)
    await cb.answer()


# ---- Close / Reopen ----
async def _close_offer(target, offer_id: int, user_id: int):
    if not is_admin(user_id):
        await target.answer("Немає прав.")
        return
    offer = db.get_offer(offer_id)
    if not offer:
        await target.answer(f"⚠ Оголошення #{offer_id} не знайдено.")
        return
    db.set_offer_status(offer_id, "closed")
    offer = db.get_offer(offer_id)
    await update_channel_post(offer)
    await target.answer(f"🔴 Оголошення <b>#{offer_id}</b> закрито.")


async def _reopen_offer(target, offer_id: int, user_id: int):
    if not is_admin(user_id):
        await target.answer("Немає прав.")
        return
    offer = db.get_offer(offer_id)
    if not offer:
        await target.answer(f"⚠ Оголошення #{offer_id} не знайдено.")
        return
    new_status = "in_progress" if db.count_proposals(offer_id) > 0 else "open"
    db.set_offer_status(offer_id, new_status)
    offer = db.get_offer(offer_id)
    await update_channel_post(offer)
    await target.answer(
        f"{status_emoji(new_status)} Оголошення <b>#{offer_id}</b> — "
        f"{status_text(new_status)}."
    )


@router.message(Command("close"))
async def cmd_close(message: Message, command: CommandObject):
    if not command.args:
        await message.answer("Використання: <code>/close &lt;номер&gt;</code>")
        return
    try:
        offer_id = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    await _close_offer(message, offer_id, message.from_user.id)


@router.message(Command("reopen"))
async def cmd_reopen(message: Message, command: CommandObject):
    if not command.args:
        await message.answer("Використання: <code>/reopen &lt;номер&gt;</code>")
        return
    try:
        offer_id = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    await _reopen_offer(message, offer_id, message.from_user.id)


@router.callback_query(F.data.startswith("close:"))
async def cb_close(cb: CallbackQuery):
    offer_id = int(cb.data.split(":")[1])
    await _close_offer(cb.message, offer_id, cb.from_user.id)
    # Оновлюємо inline-кнопку під повідомленням списку
    offer = db.get_offer(offer_id)
    if offer:
        try:
            await cb.message.edit_reply_markup(
                reply_markup=offer_actions_inline(offer)
            )
        except Exception:
            pass
    await cb.answer("Закрито")


@router.callback_query(F.data.startswith("reopen:"))
async def cb_reopen(cb: CallbackQuery):
    offer_id = int(cb.data.split(":")[1])
    await _reopen_offer(cb.message, offer_id, cb.from_user.id)
    offer = db.get_offer(offer_id)
    if offer:
        try:
            await cb.message.edit_reply_markup(
                reply_markup=offer_actions_inline(offer)
            )
        except Exception:
            pass
    await cb.answer()


# ══════════════════════  КЕРУВАННЯ АДМІНАМИ (супер-адмін)  ══════════════════════

async def _show_admins(target_message: Message, viewer_id: int):
    if not is_admin(viewer_id):
        return
    admins = db.list_admins()
    # Показуємо і супер-адміна
    super_row = {
        "user_id": SUPER_ADMIN_ID,
        "username": "",
        "first_name": "Супер-адмін",
    }
    all_list = [super_row] + [a for a in admins if a["user_id"] != SUPER_ADMIN_ID]

    lines = [f"👥 <b>Адміністраторів: {len(all_list)}</b>\n"]
    for a in all_list:
        tag = "⭐ супер" if a["user_id"] == SUPER_ADMIN_ID else "  "
        if a["username"]:
            profile = user_profile_link(
                a["user_id"], a["username"], a["first_name"]
            )
        else:
            profile = (
                html_escape(a["first_name"] or f"id{a['user_id']}")
                + f" (<code>{a['user_id']}</code>)"
            )
        lines.append(f"{tag} {profile}")

    kb = admins_list_inline(
        all_list, SUPER_ADMIN_ID, is_super_admin(viewer_id)
    )
    await target_message.answer(
        "\n".join(lines),
        reply_markup=kb,
        disable_web_page_preview=True,
    )


@router.message(F.text == ADMIN_BTN_ADMINS)
async def btn_admins(message: Message):
    if not is_super_admin(message.from_user.id):
        return
    await _show_admins(message, message.from_user.id)


@router.message(Command("admins"))
async def cmd_admins(message: Message):
    if not is_admin(message.from_user.id):
        return
    await _show_admins(message, message.from_user.id)


@router.callback_query(F.data == "addadmin")
async def cb_addadmin(cb: CallbackQuery, state: FSMContext):
    if not is_super_admin(cb.from_user.id):
        await cb.answer("Тільки супер-адмін.", show_alert=True)
        return
    await state.set_state(AddAdminStates.waiting_for_user)
    await cb.message.answer(
        "➕ <b>Додавання адміністратора</b>\n\n"
        "Варіант 1: перешліть мені будь-яке повідомлення від особи, "
        "яку хочете додати.\n\n"
        "Варіант 2: надішліть її Telegram ID (цифрами).\n\n"
        "Щоб користувач побачив свій ID, попросіть його надіслати боту /start — "
        "бот покаже ID у відповіді.",
        reply_markup=new_offer_keyboard(),  # кнопка «Скасувати»
    )
    await cb.answer()


@router.message(AddAdminStates.waiting_for_user)
async def add_admin_got_input(message: Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    user_id = None
    username = ""
    first_name = ""

    # 1) Переслане повідомлення від відкритого профілю
    if isinstance(message.forward_origin, MessageOriginUser):
        u = message.forward_origin.sender_user
        user_id = u.id
        username = u.username or ""
        first_name = u.first_name or ""
    # 2) Текст — вважаємо що це ID
    elif message.text and message.text.strip().isdigit():
        user_id = int(message.text.strip())
    else:
        await message.answer(
            "⚠ Не вдалось розпізнати. Перешліть повідомлення від особи "
            "або надішліть тільки цифри її ID."
        )
        return

    if user_id == SUPER_ADMIN_ID:
        await message.answer(
            "Ви і так супер-адмін — додавати не потрібно.",
            reply_markup=admin_menu_keyboard(message.from_user.id),
        )
        await state.clear()
        return

    # Якщо користувач уже писав боту, спробуємо підтягти дані з users
    u = db.get_user(user_id)
    if u:
        username = username or u.get("username", "")
        first_name = first_name or u.get("first_name", "")

    db.add_admin(user_id, username, first_name)
    await state.clear()

    display = (
        f"@{username}" if username else (first_name or f"id{user_id}")
    )
    await message.answer(
        f"✅ Додано адміністратора: <b>{html_escape(display)}</b> "
        f"(<code>{user_id}</code>)",
        reply_markup=admin_menu_keyboard(message.from_user.id),
    )

    # Спробуємо сповістити нового адміна
    try:
        await bot.send_message(
            user_id,
            "👋 Вас додано адміністратором бота.\n"
            "Натисніть /start щоб побачити меню.",
        )
    except Exception:
        await message.answer(
            "ℹ Сповіщення не доставлено — можливо, користувач ще "
            "не запускав бот (/start). Попросіть його натиснути /start."
        )


@router.callback_query(F.data.startswith("rmadmin:"))
async def cb_rmadmin(cb: CallbackQuery):
    if not is_super_admin(cb.from_user.id):
        await cb.answer("Тільки супер-адмін.", show_alert=True)
        return
    user_id = int(cb.data.split(":")[1])
    if user_id == SUPER_ADMIN_ID:
        await cb.answer("Супер-адміна не можна видалити.", show_alert=True)
        return
    removed = db.remove_admin(user_id)
    if removed:
        await cb.answer("Адміна видалено.")
        # Оновлюємо список
        await _show_admins(cb.message, cb.from_user.id)
    else:
        await cb.answer("Не знайдено.", show_alert=True)


# ──────────── Команди-аліаси ────────────
@router.message(Command("addadmin"))
async def cmd_addadmin(message: Message, command: CommandObject):
    if not is_super_admin(message.from_user.id):
        await message.answer("Тільки супер-адмін.")
        return
    if not command.args:
        await message.answer(
            "Використання: <code>/addadmin &lt;user_id&gt;</code>\n"
            "Або натисніть 👥 Адміни → ➕ Додати адміна."
        )
        return
    try:
        uid = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    u = db.get_user(uid)
    db.add_admin(
        uid,
        (u or {}).get("username", ""),
        (u or {}).get("first_name", ""),
    )
    await message.answer(
        f"✅ Додано адміна <code>{uid}</code>.",
        reply_markup=admin_menu_keyboard(message.from_user.id),
    )


@router.message(Command("rmadmin"))
async def cmd_rmadmin(message: Message, command: CommandObject):
    if not is_super_admin(message.from_user.id):
        await message.answer("Тільки супер-адмін.")
        return
    if not command.args:
        await message.answer("Використання: <code>/rmadmin &lt;user_id&gt;</code>")
        return
    try:
        uid = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    if uid == SUPER_ADMIN_ID:
        await message.answer("Супер-адміна не можна видалити.")
        return
    removed = db.remove_admin(uid)
    await message.answer(
        "✅ Видалено." if removed else "⚠ Адміна з таким ID немає."
    )


# ══════════════════════  ПЕРЕВІЗНИК: ВВЕДЕННЯ ЦІНИ + КОНТАКТ  ══════════════════════

@router.message(F.text == BTN_WITH_VAT)
async def price_with_vat_start(message: Message, state: FSMContext):
    data = await state.get_data()
    if "current_offer_id" not in data:
        await message.answer(
            "⚠ Спочатку оберіть оголошення, натиснувши кнопку під постом у каналі."
        )
        return
    offer = db.get_offer(data["current_offer_id"])
    if offer and offer["status"] == "closed":
        await message.answer(
            "⚠ Це оголошення вже закрито.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    await state.set_state(PriceInputStates.with_vat)
    await message.answer(
        "Введіть вартість перевезення <b>з ПДВ</b>, грн/тонну:",
        reply_markup=price_input_keyboard(),
    )


@router.message(F.text == BTN_WITHOUT_VAT)
async def price_without_vat_start(message: Message, state: FSMContext):
    data = await state.get_data()
    if "current_offer_id" not in data:
        await message.answer(
            "⚠ Спочатку оберіть оголошення, натиснувши кнопку під постом у каналі."
        )
        return
    offer = db.get_offer(data["current_offer_id"])
    if offer and offer["status"] == "closed":
        await message.answer(
            "⚠ Це оголошення вже закрито.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    await state.set_state(PriceInputStates.without_vat)
    await message.answer(
        "Введіть вартість перевезення <b>без ПДВ</b>, грн/тонну:",
        reply_markup=price_input_keyboard(),
    )


async def _save_price(
    message: Message,
    state: FSMContext,
    field: str,
    kind_label: str,
):
    price = parse_price(message.text)
    if price is None:
        await message.answer("⚠ Введіть число, наприклад: 1500 або 1500.50")
        return
    data = await state.get_data()
    request_id = data.get("current_request_id")
    offer_id = data.get("current_offer_id")
    if not request_id or not offer_id:
        await message.answer("⚠ Сесія втрачена. Перейдіть з каналу ще раз.")
        await state.clear()
        return

    if field == "price_with_vat":
        db.update_proposal_price(request_id, price_with_vat=price)
    else:
        db.update_proposal_price(request_id, price_without_vat=price)

    proposal = db.get_proposal(request_id)
    offer = db.get_offer(offer_id)

    if offer["status"] == "open":
        db.set_offer_status(offer_id, "in_progress")
        offer = db.get_offer(offer_id)
        await update_channel_post(offer)

    await state.set_state(None)
    await state.update_data(
        current_offer_id=offer_id, current_request_id=request_id
    )

    await message.answer(
        format_offer_for_carrier(offer, request_id, proposal),
        reply_markup=carrier_card_keyboard(),
        disable_web_page_preview=True,
    )
    await notify_admins_new_price(
        offer, proposal, message.from_user, kind_label, price
    )


@router.message(PriceInputStates.with_vat)
async def price_with_vat_save(message: Message, state: FSMContext):
    await _save_price(message, state, "price_with_vat", "з ПДВ")


@router.message(PriceInputStates.without_vat)
async def price_without_vat_save(message: Message, state: FSMContext):
    await _save_price(message, state, "price_without_vat", "без ПДВ")


# ---- /contact і кнопка «Передати контакт» ----
async def _show_contact_request(message: Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(
                text="📱 Надіслати мій контакт", request_contact=True
            )
        ], [KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )
    await message.answer(
        "Натисніть кнопку <b>«📱 Надіслати мій контакт»</b>, щоб передати "
        "ваш номер адміністратору:",
        reply_markup=keyboard,
    )


@router.message(Command("contact"))
async def cmd_contact(message: Message):
    await _show_contact_request(message)


@router.message(F.text == BTN_CONTACT)
async def btn_contact(message: Message):
    await _show_contact_request(message)


@router.message(F.contact)
async def got_contact(message: Message, state: FSMContext):
    contact = message.contact
    if contact.user_id and contact.user_id != message.from_user.id:
        await message.answer("⚠ Можна передати тільки свій контакт.")
        return
    db.update_user_phone(message.from_user.id, contact.phone_number)
    data = await state.get_data()
    await message.answer(
        f"✅ Контакт отримано: <b>{html_escape(contact.phone_number)}</b>",
        reply_markup=(
            carrier_card_keyboard()
            if data.get("current_offer_id")
            else ReplyKeyboardRemove()
        ),
    )
    # Сповіщаємо усіх адмінів
    profile = user_profile_link(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name,
    )
    admin_ids = {SUPER_ADMIN_ID} | {a["user_id"] for a in db.list_admins()}
    for aid in admin_ids:
        try:
            await bot.send_message(
                aid,
                f"📱 Новий контакт: {profile} → "
                f"<b>{html_escape(contact.phone_number)}</b>",
                disable_web_page_preview=True,
            )
        except Exception:
            pass


# ══════════════════════  Main  ══════════════════════

async def main():
    logger.info("Ініціалізація БД…")
    db.init()
    logger.info("Старт бота. Супер-адмін: %s", SUPER_ADMIN_ID)
    await bot.delete_webhook(drop_pending_updates=False)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
