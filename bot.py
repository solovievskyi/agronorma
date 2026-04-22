"""
Telegram-бот для роботи з перевізниками (канал + бот).

Логіка повторює схему MK Merchants:
  - адмін команди /new створює оголошення → бот публікує в каналі
    з inline-кнопкою "Ввести пропозицію".
  - перевізник клацає кнопку → відкриває бот через deep link
    ?start=offer_<id> → бот показує картку заявки з двома кнопками:
    "Ввести тариф з ПДВ" / "Ввести тариф без ПДВ".
  - адмін отримує миттєве сповіщення про кожну нову пропозицію.
  - /report <id> — зведений звіт з найкращою ціною.
  - /close <id>  — закриття оголошення (🟢 → 🔴 у пості).
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
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from database import Database

# ──────────────────────────  Конфігурація  ──────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
DB_PATH = os.getenv("DB_PATH", "bot.db")

if not BOT_TOKEN or not ADMIN_ID or not CHANNEL_ID or not BOT_USERNAME:
    raise RuntimeError(
        "Задайте змінні середовища BOT_TOKEN, ADMIN_ID, CHANNEL_ID, BOT_USERNAME"
    )

# ──────────────────────────  Логування  ──────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ──────────────────────────  Ініціалізація  ──────────────────────────
db = Database(DB_PATH)
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


# ──────────────────────────  FSM-стани  ──────────────────────────
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


# ──────────────────────────  Утиліти  ──────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


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
    """Побудувати URL-посилання на пост у приватному каналі."""
    if not channel_message_id:
        return ""
    cid = str(CHANNEL_ID).replace("-100", "", 1)
    return f"https://t.me/c/{cid}/{channel_message_id}"


def format_offer_for_channel(offer: dict) -> str:
    return (
        f"<b>Оголошення #{offer['id']}</b> "
        f"{status_emoji(offer['status'])} {status_text(offer['status'])}\n"
        f"<u>Опис перевезення:</u>\n"
        f"{offer['route_from']} - {offer['route_to']}\n"
        f"{fmt_weight(offer['weight_t'])} т\n"
        f"{offer['cargo']}\n"
        f"<u>Тариф грн/т вказує перевізник в боті</u>\n\n"
        f"<b>Додатково:</b>\n"
        f"{offer['extra_info'] or '—'}\n"
        f"{offer['contact_phone']} {offer['contact_name']}"
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
        f"Статус: {status_emoji(offer['status'])} {status_text(offer['status'])}\n\n"
        f"<u>Опис перевезення:</u>\n"
        f"{offer['route_from']} - {offer['route_to']}\n"
        f"{fmt_weight(offer['weight_t'])} т\n"
        f"{offer['cargo']}\n"
        f"Тариф перевізника з ПДВ: {price_str(p_with)}\n"
        f"Тариф перевізника без ПДВ: {price_str(p_without)}\n\n"
        f"<b>Додатково:</b>\n"
        f"{offer['extra_info'] or '—'}\n"
        f"{offer['contact_phone']} {offer['contact_name']}"
    )


BTN_WITH_VAT = "📝 Ввести тариф на перевезення з ПДВ"
BTN_WITHOUT_VAT = "📝 Ввести тариф на перевезення без ПДВ"


def carrier_card_keyboard(is_closed: bool = False) -> ReplyKeyboardMarkup | ReplyKeyboardRemove:
    if is_closed:
        return ReplyKeyboardRemove()
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WITH_VAT)],
            [KeyboardButton(text=BTN_WITHOUT_VAT)],
        ],
        resize_keyboard=True,
    )


def channel_offer_keyboard(offer_id: int) -> InlineKeyboardMarkup:
    url = f"https://t.me/{BOT_USERNAME}?start=offer_{offer_id}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="💵 Ввести вашу пропозицію щодо тарифу",
                url=url,
            )]
        ]
    )


def parse_price(text: str) -> Optional[float]:
    try:
        price = float(text.replace(",", ".").replace(" ", ""))
        if price <= 0:
            return None
        return price
    except ValueError:
        return None


async def update_channel_post(offer: dict):
    """Редагує вже опублікований пост (статус, текст)."""
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
        # Якщо повідомлення 48+ годин — Telegram може не дозволити редагувати.
        logger.warning("Не вдалось оновити пост у каналі: %s", e)


async def notify_admin_new_price(
    offer: dict, proposal: dict, user, kind: str, price: float
):
    uname = (
        f"@{user.username}"
        if user.username
        else (user.first_name or f"id{user.id}")
    )
    text = (
        f"💰 <b>Нова пропозиція</b> по оголошенню #{offer['id']}\n"
        f"Перевізник: {uname}\n"
        f"Заявка #{proposal['id']}\n"
        f"Тариф {kind}: <b>{fmt_weight(price)} грн/т</b>\n\n"
        f"Поточні ціни у цій заявці:\n"
        f"  з ПДВ: "
        f"{fmt_weight(proposal['price_with_vat']) + ' грн/т' if proposal['price_with_vat'] is not None else '—'}\n"
        f"  без ПДВ: "
        f"{fmt_weight(proposal['price_without_vat']) + ' грн/т' if proposal['price_without_vat'] is not None else '—'}"
    )
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        logger.exception("Не вдалось сповістити адміна")


# ══════════════════════════  АДМІН  ══════════════════════════

@router.message(Command("new"))
async def cmd_new(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Ця команда доступна тільки адміністратору.")
        return
    await state.clear()
    await state.set_state(NewOfferStates.route_from)
    await message.answer(
        "🆕 <b>Створення нового оголошення</b>\n\n"
        "Введіть маршрут <b>ЗВІДКИ</b>:\n"
        "(/cancel щоб скасувати)",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❎ Скасовано.", reply_markup=ReplyKeyboardRemove())


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
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception as e:
        logger.exception("Не вдалось опублікувати в канал")
        await message.answer(
            f"⚠ Не вдалось опублікувати в канал: <code>{e}</code>\n"
            f"Перевірте, що бот є адміністратором каналу з правом "
            f"«Публікувати повідомлення» і що CHANNEL_ID коректний."
        )


@router.message(Command("list"))
async def cmd_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    offers = db.list_offers(statuses=["open", "in_progress"])
    if not offers:
        await message.answer("Немає активних оголошень.")
        return
    lines = ["<b>Активні оголошення:</b>\n"]
    for o in offers:
        cnt = db.count_proposals(o["id"])
        lines.append(
            f"{status_emoji(o['status'])} <b>#{o['id']}</b> "
            f"{o['route_from']} → {o['route_to']} "
            f"({o['cargo']}, {fmt_weight(o['weight_t'])}т) — "
            f"{cnt} пропозицій"
        )
    await message.answer("\n".join(lines))


@router.message(Command("report"))
async def cmd_report(message: Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Використання: <code>/report &lt;номер_оголошення&gt;</code>")
        return
    try:
        offer_id = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    offer = db.get_offer(offer_id)
    if not offer:
        await message.answer(f"⚠ Оголошення #{offer_id} не знайдено.")
        return

    proposals = db.list_proposals(offer_id)
    if not proposals:
        await message.answer(
            f"По оголошенню #{offer_id} поки немає пропозицій."
        )
        return

    def p_str(v):
        return f"{fmt_weight(v)} грн/т" if v is not None else "—"

    def min_price(p):
        vals = [v for v in (p["price_with_vat"], p["price_without_vat"]) if v is not None]
        return min(vals) if vals else float("inf")

    sorted_p = sorted(proposals, key=min_price)

    lines = [
        f"📊 <b>Звіт по оголошенню #{offer_id}</b>",
        f"{offer['route_from']} → {offer['route_to']} | "
        f"{offer['cargo']} {fmt_weight(offer['weight_t'])}т",
        f"Статус: {status_emoji(offer['status'])} {status_text(offer['status'])}",
        f"Пропозицій: <b>{len(proposals)}</b>",
        "",
        "<b>Пропозиції (від найнижчої):</b>",
    ]
    for i, p in enumerate(sorted_p, 1):
        uname = (
            f"@{p['username']}"
            if p["username"]
            else (p["first_name"] or f"id{p['user_id']}")
        )
        phone = f" | 📱 {p['phone']}" if p["phone"] else ""
        lines.append(
            f"{i}. <b>Заявка #{p['id']}</b> — {uname}\n"
            f"   з ПДВ: {p_str(p['price_with_vat'])} | "
            f"без ПДВ: {p_str(p['price_without_vat'])}{phone}"
        )

    with_vat = [p for p in proposals if p["price_with_vat"] is not None]
    without_vat = [p for p in proposals if p["price_without_vat"] is not None]
    lines.append("")
    if with_vat:
        best = min(with_vat, key=lambda p: p["price_with_vat"])
        uname = f"@{best['username']}" if best["username"] else best["first_name"]
        lines.append(
            f"🏆 <b>Найкраща з ПДВ:</b> {fmt_weight(best['price_with_vat'])} "
            f"грн/т ({uname}, заявка #{best['id']})"
        )
    if without_vat:
        best = min(without_vat, key=lambda p: p["price_without_vat"])
        uname = f"@{best['username']}" if best["username"] else best["first_name"]
        lines.append(
            f"🏆 <b>Найкраща без ПДВ:</b> {fmt_weight(best['price_without_vat'])} "
            f"грн/т ({uname}, заявка #{best['id']})"
        )

    await message.answer("\n".join(lines))


@router.message(Command("close"))
async def cmd_close(message: Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Використання: <code>/close &lt;номер&gt;</code>")
        return
    try:
        offer_id = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    offer = db.get_offer(offer_id)
    if not offer:
        await message.answer(f"⚠ Оголошення #{offer_id} не знайдено.")
        return
    db.set_offer_status(offer_id, "closed")
    offer = db.get_offer(offer_id)
    await update_channel_post(offer)
    await message.answer(f"🔴 Оголошення <b>#{offer_id}</b> закрито.")


@router.message(Command("reopen"))
async def cmd_reopen(message: Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Використання: <code>/reopen &lt;номер&gt;</code>")
        return
    try:
        offer_id = int(command.args.strip())
    except ValueError:
        await message.answer("⚠ Введіть число.")
        return
    offer = db.get_offer(offer_id)
    if not offer:
        await message.answer(f"⚠ Оголошення #{offer_id} не знайдено.")
        return
    new_status = "in_progress" if db.count_proposals(offer_id) > 0 else "open"
    db.set_offer_status(offer_id, new_status)
    offer = db.get_offer(offer_id)
    await update_channel_post(offer)
    await message.answer(
        f"{status_emoji(new_status)} Оголошення <b>#{offer_id}</b> — "
        f"{status_text(new_status)}."
    )


# ══════════════════════════  ПЕРЕВІЗНИК  ══════════════════════════

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
        await message.answer(
            "👋 <b>Адмін-панель</b>\n\n"
            "Команди:\n"
            "<code>/new</code> — створити оголошення\n"
            "<code>/list</code> — активні оголошення\n"
            "<code>/report &lt;id&gt;</code> — звіт по оголошенню\n"
            "<code>/close &lt;id&gt;</code> — закрити оголошення\n"
            "<code>/reopen &lt;id&gt;</code> — повернути активний статус",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await message.answer(
            "👋 Вітаємо! Цей бот приймає ваші пропозиції щодо тарифів "
            "на перевезення.\n\n"
            "Щоб надіслати пропозицію — натисніть кнопку "
            "<b>«Ввести вашу пропозицію щодо тарифу»</b> під оголошенням "
            "у каналі.\n\n"
            "Команди:\n"
            "<code>/contact</code> — передати свій контактний номер\n"
            "<code>/reset</code> — вийти з режиму введення ціни"
        )


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
        await message.answer("⚠ Це оголошення вже закрито.", reply_markup=ReplyKeyboardRemove())
        return
    await state.set_state(PriceInputStates.with_vat)
    await message.answer(
        "Введіть вартість перевезення <b>з ПДВ</b>, грн/тонну:"
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
        await message.answer("⚠ Це оголошення вже закрито.", reply_markup=ReplyKeyboardRemove())
        return
    await state.set_state(PriceInputStates.without_vat)
    await message.answer(
        "Введіть вартість перевезення <b>без ПДВ</b>, грн/тонну:"
    )


@router.message(Command("reset"))
async def cmd_reset(message: Message, state: FSMContext):
    data = await state.get_data()
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
            "❎ Вийшли з режиму введення ціни.",
            reply_markup=ReplyKeyboardRemove(),
        )


async def _save_price(
    message: Message,
    state: FSMContext,
    field: str,  # "price_with_vat" або "price_without_vat"
    kind_label: str,  # "з ПДВ" / "без ПДВ"
):
    price = parse_price(message.text)
    if price is None:
        await message.answer("⚠ Введіть число, наприклад: 1500 або 1500.50")
        return
    data = await state.get_data()
    request_id = data.get("current_request_id")
    offer_id = data.get("current_offer_id")
    if not request_id or not offer_id:
        await message.answer(
            "⚠ Сесія втрачена. Перейдіть з каналу ще раз."
        )
        await state.clear()
        return

    if field == "price_with_vat":
        db.update_proposal_price(request_id, price_with_vat=price)
    else:
        db.update_proposal_price(request_id, price_without_vat=price)

    proposal = db.get_proposal(request_id)
    offer = db.get_offer(offer_id)

    # Якщо було "відкрито" — переходимо в "в роботі"
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
    await notify_admin_new_price(
        offer, proposal, message.from_user, kind_label, price
    )


@router.message(PriceInputStates.with_vat)
async def price_with_vat_save(message: Message, state: FSMContext):
    await _save_price(message, state, "price_with_vat", "з ПДВ")


@router.message(PriceInputStates.without_vat)
async def price_without_vat_save(message: Message, state: FSMContext):
    await _save_price(message, state, "price_without_vat", "без ПДВ")


@router.message(Command("contact"))
async def cmd_contact(message: Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(text="📱 Надіслати мій контакт", request_contact=True)
        ]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Натисніть кнопку, щоб передати ваш контактний номер:",
        reply_markup=keyboard,
    )


@router.message(F.contact)
async def got_contact(message: Message, state: FSMContext):
    contact = message.contact
    if contact.user_id and contact.user_id != message.from_user.id:
        await message.answer("⚠ Можна передати тільки свій контакт.")
        return
    db.update_user_phone(message.from_user.id, contact.phone_number)
    data = await state.get_data()
    await message.answer(
        f"✅ Контакт отримано: {contact.phone_number}",
        reply_markup=(
            carrier_card_keyboard()
            if data.get("current_offer_id")
            else ReplyKeyboardRemove()
        ),
    )
    try:
        uname = (
            f"@{message.from_user.username}"
            if message.from_user.username
            else message.from_user.first_name or "—"
        )
        await bot.send_message(
            ADMIN_ID,
            f"📱 Новий контакт від {uname}: <b>{contact.phone_number}</b>",
        )
    except Exception:
        logger.exception("Не вдалось сповістити адміна про контакт")


# ══════════════════════════  Main  ══════════════════════════

async def main():
    logger.info("Ініціалізація БД…")
    db.init()
    logger.info("Старт бота…")
    await bot.delete_webhook(drop_pending_updates=False)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
