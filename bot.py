"""
StashKeeper main bot — UI-driven (Select + Modal).
Includes extended !статус with cancel buttons.
"""

import asyncio
import logging
import uuid
import io
from datetime import datetime, timezone
from typing import Dict, Any

import discord
from discord.ext import commands
from discord.ui import View, Select, Modal, TextInput, Button

from sheets_adapter import SheetsAdapter
from drive_uploader import upload_bytes
from queue_manager import QueueManager
import config

logging.basicConfig(level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))
logger = logging.getLogger("stashkeeper")

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix=config.COMMAND_PREFIX, intents=intents)

sheets = None
queue = None

PENDING_REQUESTS: Dict[int, Dict[str, Any]] = {}
ACTIVE_SESSIONS: Dict[int, bool] = {}  # user_id -> True если в процессе создания заявки
sheets_lock = asyncio.Lock()

BLUE_RESOURCES = [
    "Петля Настойчивости",
    "Глаз Хаоса",
    "Сущность Магии",
    "Зеркало Гармонии",
    "Кровь Пророка",
    "Древняя Табличка"
]
PURPLE_RESOURCES = [
    "Крепкое кольцо настойчивости",
    "Горящий глаз хаоса",
    "Таинственная эссенция Магии",
    "Ослепительное зеркало Гармонии",
    "Кровь Благородного Пророка",
    "Сияющая Древняя Табличка"
]
ALL_RESOURCES = [("Blue", BLUE_RESOURCES), ("Purple", PURPLE_RESOURCES)]

def is_verifier(member: discord.Member):
    return any(r.id == config.VERIFIER_ROLE_ID for r in member.roles)

def init_adapters():
    global sheets, queue
    if sheets is None:
        sheets = SheetsAdapter(creds_file=config.GOOGLE_CREDENTIALS_FILE)
    if queue is None:
        queue = QueueManager(sheets)

@bot.event
async def on_ready():
    logger.info("Bot ready: %s", bot.user)
    init_adapters()

# ----- Admin commands -----
@bot.command(name="start_stashkeep")
@commands.has_permissions(administrator=True)
async def start_stashkeep(ctx, channel: discord.TextChannel = None):
    target = channel or ctx.channel
    await target.send("StashKeeper активирован в этом канале. Используй команду `!запрос` для создания заявки.")
    await ctx.message.add_reaction("✅")

@bot.command(name="stop_stashkeep")
@commands.has_permissions(administrator=True)
async def stop_stashkeep(ctx, channel: discord.TextChannel = None):
    target = channel or ctx.channel
    await target.send("StashKeeper отключён в этом канале.")
    await ctx.message.add_reaction("✅")

# ----- UI flow -----
class ResourceSelect(View):
    def __init__(self, author: discord.Member):
        super().__init__(timeout=120)
        self.author = author

        # Создаем опции для селекта
        options = []
        for grade, lst in ALL_RESOURCES:
            for res in lst:
                label = f"{res} ({grade})"
                # Используем простой разделитель
                options.append(discord.SelectOption(label=label, value=f"{grade}_{res}"))

        # Создаем селект
        select = Select(
            placeholder="Выберите ресурс (грейд в скобках)",
            min_values=1,
            max_values=1,
            options=options
        )

        # Назначаем callback
        async def select_callback(interaction: discord.Interaction):
            if interaction.user.id != self.author.id:
                await interaction.response.send_message("Это меню не для вас.", ephemeral=True)
                return

            val = select.values[0]
            # Разделяем значение
            parts = val.split("_", 1)
            if len(parts) == 2:
                grade, resource = parts
            else:
                # Fallback на случай ошибки
                grade = "Blue"
                resource = val

            modal = RequestModal(grade=grade, resource=resource, author=self.author)
            await interaction.response.send_modal(modal)
            self.stop()

        select.callback = select_callback
        self.add_item(select)

    async def on_timeout(self):
        # Очищаем сессию при таймауте
        ACTIVE_SESSIONS.pop(self.author.id, None)

class RequestModal(Modal):
    def __init__(self, grade: str, resource: str, author: discord.Member):
        super().__init__(title=f"Запрос: {resource}")
        self.grade = grade
        self.resource = resource
        self.author = author
        self.character = TextInput(label="Имя персонажа", placeholder="Например: Ivan")
        self.quantity = TextInput(label="Количество", placeholder="Число, например 1", max_length=6)
        self.add_item(self.character)
        self.add_item(self.quantity)

    async def on_submit(self, interaction: discord.Interaction):
        # Очищаем сессию при успешном отправлении
        ACTIVE_SESSIONS.pop(interaction.user.id, None)

        try:
            qty = int(self.quantity.value.strip())
            if qty <= 0:
                raise ValueError()
        except Exception:
            await interaction.response.send_message("Неверное количество.", ephemeral=True)
            return

        if self.grade.lower().startswith("purple"):
            await interaction.response.send_message("Пожалуйста, отправьте в этот канал изображение (вложение). Напишите 'отмена' чтобы отменить. У вас 2 минуты.", ephemeral=False)
            bot.loop.create_task(wait_for_screenshot_and_register(interaction.channel, interaction.user, self.grade, self.resource, self.character.value.strip(), qty))
        else:
            # Отправляем начальный ответ и создаем задачу
            await interaction.response.send_message("Обрабатываю ваш запрос...", ephemeral=True)
            bot.loop.create_task(process_blue_request(interaction, self.grade, self.resource, self.character.value.strip(), qty))

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        # Очищаем сессию при ошибке
        ACTIVE_SESSIONS.pop(interaction.user.id, None)
        await super().on_error(interaction, error)

async def process_blue_request(interaction: discord.Interaction, grade: str, resource: str, character: str, qty: int):
    """Обрабатывает запрос на синий ресурс в фоновом режиме"""
    try:
        rowid = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        msg_id = interaction.message.id if interaction.message else 0
        row = [
            now, str(interaction.user.id), str(interaction.user), character, grade, resource,
            str(qty), str(config.DEFAULT_PRIORITY), now, "", "active", str(interaction.channel.id),
            str(msg_id), rowid, "", "n/a", "", ""
        ]
        async with sheets_lock:
            sheets.append_row(row)
            sheets.recompute_queue_positions(resource)

        # Редактируем исходное сообщение с результатом
        try:
            await interaction.edit_original_response(content=f"Заявка принята: {resource} x{qty}.")
        except Exception as e:
            logger.warning("Не удалось отредактировать сообщение: %s", e)
            # Пробуем отправить новое сообщение
            try:
                await interaction.followup.send(f"Заявка принята: {resource} x{qty}.", ephemeral=True)
            except Exception:
                # Если и это не работает, отправляем в канал
                await interaction.channel.send(f"{interaction.user.mention}, ваша заявка принята: {resource} x{qty}.")

    except Exception as e:
        logger.exception("process_blue_request error: %s", e)
        try:
            await interaction.edit_original_response(content="Ошибка при добавлении заявки.")
        except Exception:
            try:
                await interaction.followup.send("Ошибка при добавлении заявки.", ephemeral=True)
            except Exception:
                await interaction.channel.send(f"{interaction.user.mention}, произошла ошибка при добавлении заявки.")

async def wait_for_screenshot_and_register(channel: discord.abc.Messageable, user: discord.User, grade: str, resource: str, character: str, qty: int):
    def check(m: discord.Message):
        return m.author.id == user.id and m.channel.id == channel.id and (m.attachments or (m.content and m.content.lower() == 'отмена'))

    try:
        msg: discord.Message = await bot.wait_for('message', timeout=120.0, check=check)
    except asyncio.TimeoutError:
        try:
            await channel.send(f"{user.mention}, время загрузки скриншота истекло. Запрос отменён.")
        except Exception:
            pass
        finally:
            ACTIVE_SESSIONS.pop(user.id, None)
        return

    if msg.content and msg.content.lower() == 'отмена':
        await channel.send(f"{user.mention}, запрос отменён.")
        ACTIVE_SESSIONS.pop(user.id, None)
        return

    if not msg.attachments:
        await channel.send(f"{user.mention}, не найдено вложение. Повторите команду `!запрос`.")
        ACTIVE_SESSIONS.pop(user.id, None)
        return

    attachment = msg.attachments[0]
    if not (attachment.content_type and attachment.content_type.startswith("image")):
        await channel.send(f"{user.mention}, приложите изображение.")
        ACTIVE_SESSIONS.pop(user.id, None)
        return

    try:
        content = await attachment.read()
    except Exception as e:
        logger.exception("attachment.read failed: %s", e)
        await channel.send(f"{user.mention}, не удалось прочитать файл.")
        ACTIVE_SESSIONS.pop(user.id, None)
        return

    try:
        # Используем локальный загрузчик для тестирования
        if hasattr(config, 'USE_LOCAL_UPLOADER') and config.USE_LOCAL_UPLOADER:
            # Сохраняем файл локально
            import os
            import uuid

            upload_dir = "uploads"
            os.makedirs(upload_dir, exist_ok=True)

            # Генерируем уникальное имя файла
            file_id = str(uuid.uuid4())
            ext = os.path.splitext(attachment.filename)[1] or ".png"
            new_filename = f"{file_id}{ext}"

            # Сохраняем файл
            filepath = os.path.join(upload_dir, new_filename)
            with open(filepath, "wb") as f:
                f.write(content)

            drive_link = f"Локальный файл: {new_filename}"
            logger.info(f"Файл сохранен локально: {filepath}")
        else:
            # Используем Google Drive
            loop = asyncio.get_event_loop()
            filename = f"{user.id}_{uuid.uuid4().hex}_{attachment.filename}"
            drive_link = await loop.run_in_executor(None, upload_bytes, filename, content, attachment.content_type)
    except Exception as e:
        logger.exception("Upload failed: %s", e)
        await channel.send(f"{user.mention}, ошибка при загрузке скриншота.")
        ACTIVE_SESSIONS.pop(user.id, None)
        return

    # append pending row
    try:
        rowid = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        row = [
            now, str(user.id), str(user), character, "Purple", resource,
            str(qty), str(config.DEFAULT_PRIORITY), now, "", "pending", str(channel.id),
            str(msg.id), rowid, drive_link, "awaiting", "", ""
        ]
        async with sheets_lock:
            sheets.append_row(row)
            # do not recompute until approved (pending may be part of queue but priority handled)

        # Отправляем файл как вложение в Discord
        file = discord.File(io.BytesIO(content), filename=attachment.filename)

        embed = discord.Embed(title="Новая фиолетовая заявка — требуется подтверждение", color=discord.Color.orange())
        embed.add_field(name="Игрок", value=f"{user.mention}", inline=False)
        embed.add_field(name="Ресурс", value=resource, inline=True)
        embed.add_field(name="Кол-во", value=str(qty), inline=True)
        embed.add_field(name="Персонаж", value=character, inline=False)
        embed.add_field(name="Скрин", value=f"Сохранено: {drive_link}", inline=False)

        info_msg = await channel.send(
            f"<@&{config.VERIFIER_ROLE_ID}> Пожалуйста подтвердите (реакция ✅).",
            embed=embed,
            file=file
        )

        await info_msg.add_reaction("✅")
        PENDING_REQUESTS[info_msg.id] = {"row_uuid": rowid, "requester_id": user.id, "channel_id": channel.id, "drive_link": drive_link}
        ACTIVE_SESSIONS.pop(user.id, None)

    except Exception as e:
        logger.exception("register pending row failed: %s", e)
        await channel.send(f"{user.mention}, ошибка при регистрации заявки.")
        ACTIVE_SESSIONS.pop(user.id, None)

# ----- Команда запрос -----
@bot.command(name="запрос")
async def cmd_request(ctx: commands.Context):
    """Инициирует процесс создания заявки через меню выбора."""
    try:
        # Проверяем, не находится ли пользователь уже в процессе создания заявки
        if ctx.author.id in ACTIVE_SESSIONS:
            await ctx.send("У вас уже есть активная сессия создания заявки. Завершите ее или подождите.")
            return

        # Отмечаем, что пользователь начал создание заявки
        ACTIVE_SESSIONS[ctx.author.id] = True

        # Создаем и отправляем меню выбора ресурса
        view = ResourceSelect(author=ctx.author)
        message = await ctx.send("Выберите ресурс для заявки:", view=view)

        # Ожидаем завершения выбора или таймаута
        try:
            await view.wait()
        except Exception as e:
            logger.exception("View wait error: %s", e)

    except Exception as e:
        logger.exception("cmd_request error: %s", e)
        ACTIVE_SESSIONS.pop(ctx.author.id, None)
        await ctx.send("Произошла ошибка при создании заявки.")

@bot.event
async def on_reaction_add(reaction: discord.Reaction, user: discord.User):
    try:
        if user.bot:
            return
        msg = reaction.message
        if reaction.emoji != "✅":
            return
        if msg.id not in PENDING_REQUESTS:
            return
        guild = msg.guild
        if not guild:
            return
        member = guild.get_member(user.id)
        if not member:
            return
        if not is_verifier(member):
            return
        meta = PENDING_REQUESTS.get(msg.id)
        if not meta:
            return
        row_uuid = meta.get("row_uuid")
        async with sheets_lock:
            rownum = sheets.get_row_number_by_rowid(row_uuid)
            if not rownum:
                await msg.channel.send("Не удалось найти запись в таблице.")
                return
            queue.approve_purple_request(rownum, approver_id=user.id)
        # notify
        requester = guild.get_member(meta.get("requester_id"))
        await msg.channel.send(f"Заявка подтверждена {user.display_name}.")
        if requester:
            try:
                await requester.send("Ваша фиолетовая заявка подтверждена — вы добавлены в очередь.")
            except Exception:
                logger.debug("Cannot DM requester.")
        del PENDING_REQUESTS[msg.id]
    except Exception as e:
        logger.exception("on_reaction_add error: %s", e)

# ----- Extended !статус with cancel buttons -----
class StatusView(View):
    def __init__(self, user_id: int, requests: list):
        super().__init__(timeout=120)
        self.user_id = user_id
        # requests is list of dicts with __row_number
        for req in requests:
            rownum = req.get("__row_number")
            resource = req.get("ResourceName")
            qty = req.get("Quantity")
            status = req.get("Status")
            label = f"{resource} x{qty} [{status}]"
            # create cancel button per request
            btn = Button(label=label, style=discord.ButtonStyle.secondary, custom_id=f"cancel::{rownum}")
            btn.callback = self._make_callback(rownum)
            self.add_item(btn)

    def _make_callback(self, rownum: int):
        async def callback(interaction: discord.Interaction):
            # Only requester (or admin) may cancel
            try:
                # fetch row to confirm owner
                row = sheets.get_row(rownum)
                owner_id = int(row.get("DiscordID") or 0)
                if interaction.user.id != owner_id and not interaction.user.guild_permissions.administrator:
                    await interaction.response.send_message("Вы не можете отменить эту заявку.", ephemeral=True)
                    return
                # cancel
                async with sheets_lock:
                    queue.cancel_request_by_row(rownum, requester_id=interaction.user.id)
                await interaction.response.send_message("Заявка отменена.", ephemeral=True)
            except Exception as e:
                logger.exception("StatusView cancel callback error: %s", e)
                await interaction.response.send_message("Ошибка при отмене заявки.", ephemeral=True)
        return callback

@bot.command(name="статус")
async def cmd_status(ctx: commands.Context):
    try:
        init_adapters()
        uid = str(ctx.author.id)
        # list_user_requests returns list with __row_number
        async with sheets_lock:
            requests = queue.list_user_requests(ctx.author.id)
        if not requests:
            await ctx.send("У вас нет активных или ожидающих заявок.")
            return
        # Build message and view
        lines = []
        for r in requests:
            resource = r.get("ResourceName")
            qty = r.get("Quantity")
            status = r.get("Status")
            rownum = r.get("__row_number")
            lines.append(f"Row {rownum}: {resource} x{qty} — Статус: {status}")
        content = "Ваши заявки:\n" + "\n".join(lines)
        view = StatusView(user_id=ctx.author.id, requests=requests)
        await ctx.send(content, view=view)
    except Exception as e:
        logger.exception("cmd_status error: %s", e)
        await ctx.send("Ошибка при получении статуса.")

# Error handlers
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("У вас нет прав для этой команды.")
        return
    logger.exception("Command error: %s", error)
    await ctx.send("Произошла ошибка при выполнении команды.")

if __name__ == "__main__":
    import sys
    if not getattr(config, "DISCORD_TOKEN", None):
        logger.error("Set DISCORD_TOKEN in config.py")
        sys.exit(1)
    init_adapters()
    bot.run(config.DISCORD_TOKEN)