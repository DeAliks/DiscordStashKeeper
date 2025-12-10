"""
StashKeeper main bot — UI-driven (Select + Modal).
Includes extended !статус with cancel buttons and queue management.
"""

import asyncio
import logging
import uuid
import io
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import discord
import self
from discord.ext import commands
from discord.ui import View, Select, Modal, TextInput, Button, button

from sheets_adapter import SheetsAdapter
from drive_uploader import upload_bytes
from queue_manager import QueueManager
from priority_manager import (
    get_user_priority, set_multiple_users_priority, remove_multiple_users_priority,
    get_all_priority_users, clear_all_priorities, HIGH_PRIORITY, DEFAULT_PRIORITY
)
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
ACTIVE_SESSIONS: Dict[int, Dict[str, Any]] = {}  # user_id -> session data
USER_COMMAND_MESSAGES: Dict[int, discord.Message] = {}  # user_id -> command message to delete
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

# ----- Priority commands -----
@bot.command(name="set_priority")
@commands.has_permissions(administrator=True)
async def cmd_set_priority(ctx: commands.Context, *members: discord.Member):
    """Устанавливает высокий приоритет для указанных пользователей"""
    try:
        if not members:
            await ctx.send("❌ Укажите пользователей через @упоминание.", ephemeral=True)
            return

        # Сохраняем сообщение команды для удаления
        USER_COMMAND_MESSAGES[ctx.author.id] = ctx.message

        user_ids = [str(member.id) for member in members]
        set_multiple_users_priority(user_ids, HIGH_PRIORITY)

        mentions = ", ".join([member.mention for member in members])
        await ctx.send(f"✅ Установлен высокий приоритет для: {mentions}", ephemeral=True)

        # Удаляем сообщение команды через 30 секунд
        await asyncio.sleep(30)
        try:
            await ctx.message.delete()
        except:
            pass

    except Exception as e:
        logger.exception("cmd_set_priority error: %s", e)
        await ctx.send("❌ Ошибка при установке приоритета.", ephemeral=True)

@bot.command(name="remove_priority")
@commands.has_permissions(administrator=True)
async def cmd_remove_priority(ctx: commands.Context, *members: discord.Member):
    """Удаляет высокий приоритет у указанных пользователей"""
    try:
        if not members:
            await ctx.send("❌ Укажите пользователей через @упоминание.", ephemeral=True)
            return

        # Сохраняем сообщение команды для удаления
        USER_COMMAND_MESSAGES[ctx.author.id] = ctx.message

        user_ids = [str(member.id) for member in members]
        remove_multiple_users_priority(user_ids)

        mentions = ", ".join([member.mention for member in members])
        await ctx.send(f"✅ Удален высокий приоритет у: {mentions}", ephemeral=True)

        # Удаляем сообщение команды через 30 секунд
        await asyncio.sleep(30)
        try:
            await ctx.message.delete()
        except:
            pass

    except Exception as e:
        logger.exception("cmd_remove_priority error: %s", e)
        await ctx.send("❌ Ошибка при удалении приоритета.", ephemeral=True)

@bot.command(name="list_priority")
@commands.has_permissions(administrator=True)
async def cmd_list_priority(ctx: commands.Context):
    """Показывает всех пользователей с высоким приоритетом"""
    try:
        # Сохраняем сообщение команды для удаления
        USER_COMMAND_MESSAGES[ctx.author.id] = ctx.message

        priority_users = get_all_priority_users()

        if not priority_users:
            await ctx.send("📋 Нет пользователей с высоким приоритетом.", ephemeral=True)
            return

        embed = discord.Embed(
            title="👑 Пользователи с высоким приоритетом",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc)
        )

        for user_id, priority in priority_users.items():
            member = ctx.guild.get_member(int(user_id))
            if member:
                embed.add_field(
                    name=member.display_name,
                    value=f"ID: {user_id} | Приоритет: {priority}",
                    inline=False
                )
            else:
                embed.add_field(
                    name=f"Неизвестный пользователь",
                    value=f"ID: {user_id} | Приоритет: {priority}",
                    inline=False
                )

        embed.set_footer(text=f"Всего: {len(priority_users)} пользователей")
        await ctx.send(embed=embed, ephemeral=True)

        # Удаляем сообщение команды через 30 секунд
        await asyncio.sleep(30)
        try:
            await ctx.message.delete()
        except:
            pass

    except Exception as e:
        logger.exception("cmd_list_priority error: %s", e)
        await ctx.send("❌ Ошибка при получении списка приоритетов.", ephemeral=True)

@bot.command(name="clear_priority")
@commands.has_permissions(administrator=True)
async def cmd_clear_priority(ctx: commands.Context):
    """Очищает все приоритеты"""
    try:
        # Сохраняем сообщение команды для удаления
        USER_COMMAND_MESSAGES[ctx.author.id] = ctx.message

        clear_all_priorities()
        await ctx.send("✅ Все приоритеты очищены.", ephemeral=True)

        # Удаляем сообщение команды через 30 секунд
        await asyncio.sleep(30)
        try:
            await ctx.message.delete()
        except:
            pass

    except Exception as e:
        logger.exception("cmd_clear_priority error: %s", e)
        await ctx.send("❌ Ошибка при очистке приоритетов.", ephemeral=True)

# ----- UI flow -----
class ResourceSelect(View):
    def __init__(self, author: discord.Member, session_id: str):
        super().__init__(timeout=120)
        self.author = author
        self.session_id = session_id

        # Проверяем приоритет пользователя
        user_priority = get_user_priority(str(author.id))
        priority_info = ""
        if user_priority > DEFAULT_PRIORITY:
            priority_info = f" (Приоритет: {user_priority})"

        # Создаем опции для селекта
        options = []
        for grade, lst in ALL_RESOURCES:
            for res in lst:
                label = f"{res} ({grade})"
                # Используем простой разделитель
                options.append(discord.SelectOption(label=label, value=f"{grade}_{res}"))

        # Создаем селект
        select = Select(
            placeholder=f"Выберите ресурс (грейд в скобках){priority_info}",
            min_values=1,
            max_values=1,
            options=options
        )

        # Назначаем callback
        async def select_callback(interaction: discord.Interaction):
            if interaction.user.id != self.author.id:
                await interaction.response.send_message("Это меню не для вас.", ephemeral=True)
                return

            # Проверяем, что сессия еще активна
            if interaction.user.id not in ACTIVE_SESSIONS:
                await interaction.response.send_message("Ваша сессия истекла. Начните заново с `!запрос`.", ephemeral=True)
                self.stop()
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

            modal = RequestModal(grade=grade, resource=resource, author=self.author, session_id=self.session_id)
            await interaction.response.send_modal(modal)
            self.stop()

        select.callback = select_callback
        self.add_item(select)

    async def on_timeout(self):
        # Очищаем сессию при таймауте
        if self.author.id in ACTIVE_SESSIONS and ACTIVE_SESSIONS[self.author.id].get("session_id") == self.session_id:
            # Удаляем сообщение с меню, если оно есть
            menu_message_id = ACTIVE_SESSIONS[self.author.id].get("menu_message_id")
            if menu_message_id:
                try:
                    channel = bot.get_channel(ACTIVE_SESSIONS[self.author.id].get("channel_id"))
                    if channel:
                        msg = await channel.fetch_message(menu_message_id)
                        await msg.delete()
                except:
                    pass

            # Удаляем сессию
            del ACTIVE_SESSIONS[self.author.id]

class RequestModal(Modal):
    def __init__(self, grade: str, resource: str, author: discord.Member, session_id: str):
        super().__init__(title=f"Запрос: {resource}")
        self.grade = grade
        self.resource = resource
        self.author = author
        self.session_id = session_id

        # Для фиолетовых ресурсов используем ник пользователя по умолчанию
        default_character = ""
        if grade.lower().startswith("purple"):
            # Берем ник без дискриминатора (части после #)
            default_character = author.name

        self.character = TextInput(
            label="Имя персонажа",
            placeholder=f"Например: {author.name}",
            default=default_character if default_character else None,
            max_length=32
        )
        self.quantity = TextInput(label="Количество", placeholder="Число, например 1", max_length=6)
        self.add_item(self.character)
        self.add_item(self.quantity)

    async def on_submit(self, interaction: discord.Interaction):
        # Проверяем, что сессия еще активна
        if interaction.user.id not in ACTIVE_SESSIONS or ACTIVE_SESSIONS[interaction.user.id].get("session_id") != self.session_id:
            await interaction.response.send_message("Ваша сессия истекла. Начните заново с `!запрос`.", ephemeral=True)
            return

        try:
            qty = int(self.quantity.value.strip())
            if qty <= 0:
                raise ValueError()
        except Exception:
            await interaction.response.send_message("Неверное количество.", ephemeral=True)
            return

        # Если имя персонажа пустое, используем ник пользователя
        character_name = self.character.value.strip()
        if not character_name:
            character_name = interaction.user.name

        if self.grade.lower().startswith("purple"):
            # Сохраняем данные заявки в сессию
            ACTIVE_SESSIONS[interaction.user.id]["request_data"] = {
                "grade": self.grade,
                "resource": self.resource,
                "character": character_name,
                "qty": qty
            }

            # Отправляем сообщение только пользователю
            await interaction.response.send_message(
                "Пожалуйста, отправьте в этот канал изображение (вложение). Напишите 'отмена' чтобы отменить. У вас 2 минуты.",
                ephemeral=True
            )

            # Запускаем ожидание скриншота
            bot.loop.create_task(wait_for_screenshot_and_register(
                interaction.channel,
                interaction.user,
                self.grade,
                self.resource,
                character_name,
                qty,
                self.session_id
            ))
        else:
            # Отправляем начальный ответ и создаем задачу для синих ресурсов
            await interaction.response.defer(ephemeral=True)
            bot.loop.create_task(process_blue_request(interaction, self.grade, self.resource, character_name, qty, self.session_id))

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        # Очищаем сессию при ошибке
        if interaction.user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[interaction.user.id]
        await super().on_error(interaction, error)

async def process_blue_request(interaction: discord.Interaction, grade: str, resource: str, character: str, qty: int, session_id: str):
    """Обрабатывает запрос на синий ресурс в фоновом режиме"""
    try:
        # Проверяем, что сессия еще активна
        if interaction.user.id not in ACTIVE_SESSIONS or ACTIVE_SESSIONS[interaction.user.id].get("session_id") != session_id:
            await interaction.followup.send("Ваша сессия истекла. Начните заново с `!запрос`.", ephemeral=True)
            return

        # Получаем приоритет пользователя
        user_priority = get_user_priority(str(interaction.user.id))

        rowid = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        msg_id = interaction.message.id if interaction.message else 0
        row = [
            now, str(interaction.user.id), str(interaction.user), character, grade, resource,
            str(qty), "0", str(qty), str(user_priority), now, "", "active", str(interaction.channel.id),
            str(msg_id), rowid, "", "n/a", "", ""
        ]

        async with sheets_lock:
            sheets.append_row(row)
            sheets.recompute_queue_positions(resource)

            # Получаем позицию в очереди
            rownum = sheets.get_row_number_by_rowid(rowid)
            if rownum:
                row_data = sheets.get_row(rownum)
                queue_position = row_data.get("QueuePosition", "?")
            else:
                queue_position = "?"

        # Отправляем приватное сообщение пользователю
        embed = discord.Embed(
            title="✅ Заявка принята",
            description=f"**{resource}** x{qty}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="👤 Ваш персонаж", value=character, inline=True)
        embed.add_field(name="📊 Позиция в очереди", value=f"№{queue_position}", inline=True)
        embed.add_field(name="🎮 Статус", value="В очереди на выдачу", inline=False)

        # Добавляем информацию о приоритете
        if user_priority > DEFAULT_PRIORITY:
            embed.add_field(name="👑 Приоритет", value=f"Уровень {user_priority}", inline=True)

        embed.set_footer(text=f"ID заявки: {rowid[:8]}")

        await interaction.followup.send(embed=embed, ephemeral=True)

        # Отправляем публичное уведомление в канал
        public_embed = discord.Embed(
            title="📋 Новая заявка в очереди",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        public_embed.add_field(name="👤 Игрок", value=f"{interaction.user.mention}", inline=True)
        public_embed.add_field(name="🎮 Персонаж", value=character, inline=True)
        public_embed.add_field(name="🔵 Ресурс", value=f"{resource} x{qty}", inline=False)
        public_embed.add_field(name="📊 Позиция", value=f"№{queue_position}", inline=True)

        # Добавляем иконку приоритета
        if user_priority > DEFAULT_PRIORITY:
            public_embed.add_field(name="👑 Приоритет", value=f"Уровень {user_priority}", inline=True)

        public_embed.set_footer(text=f"ID: {rowid[:8]}")

        public_msg = await interaction.channel.send(embed=public_embed)

        # Удаляем публичное сообщение через 30 секунд
        await asyncio.sleep(30)
        try:
            await public_msg.delete()
        except:
            pass

        # Очищаем сессию
        if interaction.user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[interaction.user.id]

    except Exception as e:
        logger.exception("process_blue_request error: %s", e)
        try:
            await interaction.followup.send("Ошибка при добавлении заявки.", ephemeral=True)
        except Exception:
            pass

        # Очищаем сессию при ошибке
        if interaction.user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[interaction.user.id]

async def wait_for_screenshot_and_register(channel: discord.abc.Messageable, user: discord.User, grade: str, resource: str, character: str, qty: int, session_id: str):
    def check(m: discord.Message):
        return m.author.id == user.id and m.channel.id == channel.id and (m.attachments or (m.content and m.content.lower() == 'отмена'))

    screenshot_request_msg = None

    try:
        # Отправляем сообщение только для этого пользователя
        screenshot_request_msg = await channel.send(f"{user.mention}, пожалуйста, отправьте скриншот в течение 2 минут. Напишите 'отмена' чтобы отменить.")

        msg: discord.Message = await bot.wait_for('message', timeout=120.0, check=check)
    except asyncio.TimeoutError:
        try:
            await channel.send(f"{user.mention}, время загрузки скриншота истекло. Запрос отменён.")
        except Exception:
            pass
        finally:
            # Удаляем сообщение с запросом скриншота
            if screenshot_request_msg:
                try:
                    await screenshot_request_msg.delete()
                except:
                    pass
            # Очищаем сессию
            if user.id in ACTIVE_SESSIONS:
                del ACTIVE_SESSIONS[user.id]
        return

    # Удаляем сообщение с запросом скриншота
    if screenshot_request_msg:
        try:
            await screenshot_request_msg.delete()
        except:
            pass

    if msg.content and msg.content.lower() == 'отмена':
        await channel.send(f"{user.mention}, запрос отменён.")
        # Очищаем сессию
        if user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user.id]
        return

    if not msg.attachments:
        await channel.send(f"{user.mention}, не найдено вложение. Повторите команду `!запрос`.")
        # Очищаем сессию
        if user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user.id]
        return

    attachment = msg.attachments[0]
    if not (attachment.content_type and attachment.content_type.startswith("image")):
        await channel.send(f"{user.mention}, приложите изображение.")
        # Очищаем сессию
        if user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user.id]
        return

    try:
        content = await attachment.read()
    except Exception as e:
        logger.exception("attachment.read failed: %s", e)
        await channel.send(f"{user.mention}, не удалось прочитать файл.")
        # Очищаем сессию
        if user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user.id]
        return

    try:
        # Используем локальный загрузчик для тестирования
        if hasattr(config, 'USE_LOCAL_UPLOADER') and config.USE_LOCAL_UPLOADER:
            # Сохраняем файл локально
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
        # Очищаем сессию
        if user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user.id]
        return

    # append pending row
    try:
        # Получаем приоритет пользователя
        user_priority = get_user_priority(str(user.id))

        rowid = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        row = [
            now, str(user.id), str(user), character, "Purple", resource,
            str(qty), "0", str(qty), str(user_priority), now, "", "pending", str(channel.id),
            str(msg.id), rowid, drive_link, "awaiting", "", ""
        ]
        async with sheets_lock:
            sheets.append_row(row)
            # do not recompute until approved (pending may be part of queue but priority handled)

        # Удаляем сообщение пользователя со скриншотом
        try:
            await msg.delete()
        except:
            pass

        # Отправляем файл как вложение в Discord
        file = discord.File(io.BytesIO(content), filename=attachment.filename)

        embed = discord.Embed(
            title="🟣 Новая фиолетовая заявка — требуется подтверждение",
            color=discord.Color.purple(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="👤 Игрок", value=f"{user.mention}", inline=False)
        embed.add_field(name="🎮 Персонаж", value=character, inline=True)
        embed.add_field(name="📦 Ресурс", value=resource, inline=True)
        embed.add_field(name="🔢 Количество", value=str(qty), inline=True)

        # Добавляем информацию о приоритете
        if user_priority > DEFAULT_PRIORITY:
            embed.add_field(name="👑 Приоритет", value=f"Уровень {user_priority}", inline=True)

        embed.add_field(name="📎 Скриншот", value=drive_link, inline=False)
        embed.set_footer(text=f"ID заявки: {rowid[:8]} • Нажмите ✅ для подтверждения")

        info_msg = await channel.send(
            f"<@&{config.VERIFIER_ROLE_ID}> Пожалуйста подтвердите заявку:",
            embed=embed,
            file=file
        )

        await info_msg.add_reaction("✅")
        PENDING_REQUESTS[info_msg.id] = {
            "row_uuid": rowid,
            "requester_id": user.id,
            "channel_id": channel.id,
            "drive_link": drive_link,
            "resource": resource,
            "character": character,
            "quantity": qty
        }

        # Очищаем сессию пользователя
        if user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user.id]

    except Exception as e:
        logger.exception("register pending row failed: %s", e)
        await channel.send(f"{user.mention}, ошибка при регистрации заявки.")
        # Очищаем сессию
        if user.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user.id]

# ----- Команда запрос -----
@bot.command(name="запрос")
async def cmd_request(ctx: commands.Context):
    """Инициирует процесс создания заявки через меню выбора."""
    try:
        # Сохраняем сообщение команды для удаления
        USER_COMMAND_MESSAGES[ctx.author.id] = ctx.message

        # Проверяем, не находится ли пользователь уже в процессе создания заявки
        if ctx.author.id in ACTIVE_SESSIONS:
            # Проверяем, не истекла ли старая сессия
            session_data = ACTIVE_SESSIONS[ctx.author.id]
            session_time = session_data.get("created_at", 0)
            if asyncio.get_event_loop().time() - session_time > 120:
                # Сессия истекла, удаляем
                del ACTIVE_SESSIONS[ctx.author.id]
            else:
                await ctx.send("У вас уже есть активная сессия создания заявки. Завершите ее или подождите.", ephemeral=True)
                return

        # Создаем новую сессию
        session_id = str(uuid.uuid4())
        ACTIVE_SESSIONS[ctx.author.id] = {
            "session_id": session_id,
            "created_at": asyncio.get_event_loop().time(),
            "channel_id": ctx.channel.id,
            "user_id": ctx.author.id
        }

        # Создаем и отправляем меню выбора ресурса (только для пользователя)
        view = ResourceSelect(author=ctx.author, session_id=session_id)
        message = await ctx.send("Выберите ресурс для заявки:", view=view, ephemeral=True)

        # Сохраняем ID сообщения с меню для возможности удаления
        ACTIVE_SESSIONS[ctx.author.id]["menu_message_id"] = message.id

        # Запускаем таймер для автоматической очистки сессии
        bot.loop.create_task(cleanup_session(ctx.author.id, session_id, message, ctx.message))

    except Exception as e:
        logger.exception("cmd_request error: %s", e)
        if ctx.author.id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[ctx.author.id]
        await ctx.send("Произошла ошибка при создании заявки.", ephemeral=True)

async def cleanup_session(user_id: int, session_id: str, menu_message: discord.Message, command_message: discord.Message):
    """Очищает сессию через 120 секунд"""
    await asyncio.sleep(120)

    if user_id in ACTIVE_SESSIONS and ACTIVE_SESSIONS[user_id].get("session_id") == session_id:
        try:
            # Удаляем сообщение с меню
            await menu_message.delete()
        except:
            pass

        # Удаляем сообщение команды
        try:
            await command_message.delete()
        except:
            pass

        # Удаляем сессию
        del ACTIVE_SESSIONS[user_id]

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

            # Получаем позицию в очереди
            row_data = sheets.get_row(rownum)
            queue_position = row_data.get("QueuePosition", "?")

        # Отправляем уведомление о подтверждении
        requester = guild.get_member(meta.get("requester_id"))
        resource = meta.get("resource")
        character = meta.get("character")
        quantity = meta.get("quantity")

        # Получаем приоритет пользователя
        user_priority = get_user_priority(str(meta.get("requester_id")))

        # Уведомление в канал (удаляем через 30 секунд)
        embed = discord.Embed(
            title="✅ Заявка подтверждена",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="👤 Игрок", value=f"<@{meta.get('requester_id')}>", inline=True)
        embed.add_field(name="🎮 Персонаж", value=character, inline=True)
        embed.add_field(name="🟣 Ресурс", value=f"{resource} x{quantity}", inline=False)
        embed.add_field(name="📊 Позиция в очереди", value=f"№{queue_position}", inline=True)

        # Добавляем информацию о приоритете
        if user_priority > DEFAULT_PRIORITY:
            embed.add_field(name="👑 Приоритет", value=f"Уровень {user_priority}", inline=True)

        embed.add_field(name="👮 Подтвердил", value=user.display_name, inline=True)
        embed.set_footer(text=f"ID заявки: {row_uuid[:8]}")

        notification_msg = await msg.channel.send(embed=embed)

        # Уведомление пользователю в ЛС
        if requester:
            try:
                user_embed = discord.Embed(
                    title="✅ Ваша заявка подтверждена",
                    description=f"**{resource}** x{quantity}",
                    color=discord.Color.green(),
                    timestamp=datetime.now(timezone.utc)
                )
                user_embed.add_field(name="👮 Подтвердил", value=user.display_name, inline=True)
                user_embed.add_field(name="📊 Позиция в очереди", value=f"№{queue_position}", inline=True)
                user_embed.add_field(name="🎮 Статус", value="В очереди на выдачу", inline=False)
                user_embed.set_footer(text=f"ID заявки: {row_uuid[:8]}")

                await requester.send(embed=user_embed)
            except Exception:
                logger.debug("Cannot DM requester.")

        # Удаляем сообщение с запросом на подтверждение
        try:
            await msg.delete()
        except:
            pass

        # Удаляем уведомление в канале через 30 секунд
        await asyncio.sleep(30)
        try:
            await notification_msg.delete()
        except:
            pass

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
            queue_pos = req.get("QueuePosition", "?")
            priority = req.get("PriorityLevel", "1")

            # Добавляем иконку приоритета
            priority_icon = ""
            if int(priority) > DEFAULT_PRIORITY:
                priority_icon = "👑 "

            label = f"{priority_icon}{resource} x{qty} [Поз.{queue_pos}]"
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
    """Показывает статус заявок пользователя."""
    try:
        # Сохраняем сообщение команды для удаления
        USER_COMMAND_MESSAGES[ctx.author.id] = ctx.message

        init_adapters()
        # list_user_requests returns list with __row_number
        async with sheets_lock:
            requests = queue.list_user_requests(ctx.author.id)
        if not requests:
            await ctx.send("У вас нет активных или ожидающих заявок.", ephemeral=True)
            return

        # Сортируем заявки по статусу (сначала активные, затем pending)
        requests.sort(key=lambda x: (0 if x.get("Status") == "active" else 1, x.get("QueuePosition", 999)))

        # Build message and view
        embed = discord.Embed(
            title="📊 Ваши заявки",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )

        active_requests = []
        pending_requests = []

        for r in requests:
            resource = r.get("ResourceName")
            qty = r.get("Quantity")
            status = r.get("Status")
            queue_pos = r.get("QueuePosition", "?")
            character = r.get("CharacterName", "?")
            grade = r.get("ResourceGrade", "Blue")
            priority = int(r.get("PriorityLevel", "1"))
            issued = r.get("IssuedQuantity", 0)
            remaining = r.get("Remaining", qty)

            status_text = "✅ Активна" if status == "active" else "⏳ Ожидает подтверждения"
            grade_emoji = "🔵" if grade.lower() == "blue" else "🟣"
            priority_icon = "👑 " if priority > DEFAULT_PRIORITY else ""

            request_info = f"{priority_icon}{grade_emoji} **{resource}** x{qty}\n"
            request_info += f"👤 {character} | 📊 Поз. {queue_pos} | {status_text}\n"
            if issued > 0:
                request_info += f"📦 Выдано: {issued}/{qty} (осталось: {remaining})\n"
            if priority > DEFAULT_PRIORITY:
                request_info += f"👑 Приоритет: {priority}\n"

            if status == "active":
                active_requests.append(request_info)
            else:
                pending_requests.append(request_info)

        if active_requests:
            embed.add_field(name="Активные заявки", value="\n".join(active_requests) or "Нет", inline=False)
        if pending_requests:
            embed.add_field(name="Ожидающие подтверждения", value="\n".join(pending_requests) or "Нет", inline=False)

        # Проверяем приоритет пользователя
        user_priority = get_user_priority(str(ctx.author.id))
        if user_priority > DEFAULT_PRIORITY:
            embed.add_field(
                name="👑 Ваш приоритет",
                value=f"Уровень {user_priority}. Ваши заявки обрабатываются в первую очередь.",
                inline=False
            )

        embed.set_footer(text=f"Всего заявок: {len(requests)}")

        view = StatusView(user_id=ctx.author.id, requests=requests)
        status_msg = await ctx.send(embed=embed, view=view, ephemeral=True)

        # Удаляем сообщение команды через 120 секунд
        await asyncio.sleep(120)
        try:
            await ctx.message.delete()
        except:
            pass

    except Exception as e:
        logger.exception("cmd_status error: %s", e)
        await ctx.send("Ошибка при получении статуса.", ephemeral=True)

# ----- Модальные окна для управления заявками -----
class IssueQuantityModal(Modal):
    """Модальное окно для выдачи произвольного количества"""

    def __init__(self, row_number: int, resource: str, player: str, max_quantity: int):
        super().__init__(title=f"Выдать ресурс: {resource}")
        self.row_number = row_number
        self.resource = resource
        self.player = player
        self.max_quantity = max_quantity

        self.quantity = TextInput(
            label="Количество для выдачи",
            placeholder=f"Введите число от 1 до {max_quantity}",
            default=str(max_quantity) if max_quantity <= 10 else "10",
            max_length=10
        )
        self.add_item(self.quantity)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            qty = int(self.quantity.value.strip())
            if qty <= 0 or qty > self.max_quantity:
                await interaction.response.send_message(
                    f"❌ Неверное количество. Введите число от 1 до {self.max_quantity}.",
                    ephemeral=True
                )
                return

            # Выдаем указанное количество
            await self._issue_quantity(interaction, self.row_number, qty)

        except ValueError:
            await interaction.response.send_message(
                "❌ Введите корректное число.",
                ephemeral=True
            )
        except Exception as e:
            logger.exception(f"Error in IssueQuantityModal: {e}")
            await interaction.response.send_message(
                "❌ Произошла ошибка при обработке запроса.",
                ephemeral=True
            )

    async def _issue_quantity(self, interaction: discord.Interaction, row_number: int, quantity: int):
        """Выдать указанное количество"""
        try:
            # === ДОБАВЛЕНА ПРОВЕРКА НА НЕВАЛИДНЫЙ НОМЕР СТРОКИ ===
            if row_number is None:
                await interaction.response.send_message(
                    "❌ Ошибка: не удалось определить номер строки заявки. Пожалуйста, выберите заявку заново.",
                    ephemeral=True
                )
                return

            row = sheets.get_row(row_number)
            if not row:
                await interaction.response.send_message(
                    "❌ Заявка не найдена в таблице.",
                    ephemeral=True
                )
                return

            current_issued = int(row.get("IssuedQuantity", 0) or 0)
            total = int(row.get("Quantity", 0) or 0)

            new_issued = min(current_issued + quantity, total)

            # Update in database
            completed = (new_issued >= total)
            success = sheets.update_issued_quantity(
                row_number,
                new_issued,
                completed=completed
            )

            if success:
                # Send success message
                remaining = total - new_issued

                if completed:
                    message = f"✅ Заявка #{row_number} полностью выполнена!\n"
                    message += f"**Ресурс:** {row.get('ResourceName')}\n"
                    message += f"**Игрок:** {row.get('DiscordName')}\n"
                    message += f"**Выдано:** {new_issued}/{total}"

                    # Notify player
                    player_id = row.get("DiscordID")
                    if player_id:
                        try:
                            guild = interaction.guild
                            member = guild.get_member(int(player_id))
                            if member:
                                embed = discord.Embed(
                                    title="🎉 Ваша заявка выполнена!",
                                    description=f"**{row.get('ResourceName')}** x{total}",
                                    color=discord.Color.green(),
                                    timestamp=datetime.now(timezone.utc)
                                )
                                embed.add_field(name="👮 Выдал", value=interaction.user.display_name, inline=True)
                                embed.add_field(name="📦 Количество", value=f"{total} единиц", inline=True)
                                embed.set_footer(text=f"ID заявки: {row.get('RowID', '')[:8]}")

                                await member.send(embed=embed)
                        except Exception:
                            logger.debug("Could not DM player")
                else:
                    message = f"✅ Выдано {quantity} единиц для заявки #{row_number}\n"
                    message += f"**Ресурс:** {row.get('ResourceName')}\n"
                    message += f"**Игрок:** {row.get('DiscordName')}\n"
                    message += f"**Прогресс:** {new_issued}/{total} (осталось: {remaining})"

                await interaction.response.send_message(message, ephemeral=True)

                # Update queue view
                await self._refresh_queue_view(interaction)
            else:
                await interaction.response.send_message(
                    "❌ Ошибка при обновлении заявки.",
                    ephemeral=True
                )

        except Exception as e:
            logger.exception(f"Error issuing quantity: {e}")
            await interaction.response.send_message(
                "❌ Произошла ошибка при выдаче ресурсов.",
                ephemeral=True
            )
    async def _refresh_queue_view(self, interaction: discord.Interaction):
        """Обновить представление очереди"""
        try:
            # Найти оригинальное сообщение с очередью и обновить его
            channel = interaction.channel
            async for message in channel.history(limit=50):
                if message.author == bot.user and message.embeds:
                    if "Управление очередью" in message.embeds[0].title:
                        # Get updated active requests
                        active_requests = sheets.get_active_requests()

                        # Recreate view
                        new_view = QueueManagementView(active_requests)
                        embed = new_view._create_embed()
                        await message.edit(embed=embed, view=new_view)
                        break
        except Exception as e:
            logger.exception(f"Error refreshing queue view: {e}")

class UnissueQuantityModal(Modal):
    """Модальное окно для возврата произвольного количества"""

    def __init__(self, row_number: int, resource: str, player: str, max_quantity: int):
        super().__init__(title=f"Вернуть ресурс: {resource}")
        self.row_number = row_number
        self.resource = resource
        self.player = player
        self.max_quantity = max_quantity

        self.quantity = TextInput(
            label="Количество для возврата",
            placeholder=f"Введите число от 1 до {max_quantity}",
            default="1",
            max_length=10
        )
        self.add_item(self.quantity)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            qty = int(self.quantity.value.strip())
            if qty <= 0 or qty > self.max_quantity:
                await interaction.response.send_message(
                    f"❌ Неверное количество. Введите число от 1 до {self.max_quantity}.",
                    ephemeral=True
                )
                return

            # Возвращаем указанное количество
            await self._unissue_quantity(interaction, self.row_number, qty)

        except ValueError:
            await interaction.response.send_message(
                "❌ Введите корректное число.",
                ephemeral=True
            )
        except Exception as e:
            logger.exception(f"Error in UnissueQuantityModal: {e}")
            await interaction.response.send_message(
                "❌ Произошла ошибка при обработке запроса.",
                ephemeral=True
            )

    async def _unissue_quantity(self, interaction: discord.Interaction, row_number: int, quantity: int):
        """Вернуть указанное количество"""
        try:
            # === ДОБАВЛЕНА ПРОВЕРКА НА НЕВАЛИДНЫЙ НОМЕР СТРОКИ ===
            if row_number is None:
                await interaction.response.send_message(
                    "❌ Ошибка: не удалось определить номер строки заявки.",
                    ephemeral=True
                )
                return

            row = sheets.get_row(row_number)
            if not row:
                await interaction.response.send_message(
                    "❌ Заявка не найдена.",
                    ephemeral=True
                )
                return

            current_issued = int(row.get("IssuedQuantity", 0) or 0)
            new_issued = max(0, current_issued - quantity)

            # Update in database
            success = sheets.update_issued_quantity(row_number, new_issued)

            if success:
                message = f"↩️ Возвращено {quantity} единиц для заявки #{row_number}\n"
                message += f"**Ресурс:** {row.get('ResourceName')}\n"
                message += f"**Игрок:** {row.get('DiscordName')}\n"
                message += f"**Текущий прогресс:** {new_issued}/{row.get('Quantity', 0)}"

                await interaction.response.send_message(message, ephemeral=True)

                # Update queue view
                await self._refresh_queue_view(interaction)
            else:
                await interaction.response.send_message(
                    "❌ Ошибка при обновлении заявки.",
                    ephemeral=True
                )

        except Exception as e:
            logger.exception(f"Error unissuing quantity: {e}")
            await interaction.response.send_message(
                "❌ Произошла ошибка.",
                ephemeral=True
            )
    async def _refresh_queue_view(self, interaction: discord.Interaction):
        """Обновить представление очереди"""
        try:
            # Найти оригинальное сообщение с очередью и обновить его
            channel = interaction.channel
            async for message in channel.history(limit=50):
                if message.author == bot.user and message.embeds:
                    if "Управление очередью" in message.embeds[0].title:
                        # Get updated active requests
                        active_requests = sheets.get_active_requests()

                        # Recreate view
                        new_view = QueueManagementView(active_requests)
                        embed = new_view._create_embed()
                        await message.edit(embed=embed, view=new_view)
                        break
        except Exception as e:
            logger.exception(f"Error refreshing queue view: {e}")

# ----- Команда для просмотра и управления очередью -----
class QueueSelectView(View):
    """View with dropdown for selecting a request to manage."""

    def __init__(self, requests: List[Dict[str, Any]]):
        super().__init__(timeout=300)
        self.requests = requests

        # Create select dropdown
        options = []
        for req in requests[:25]:  # Discord limit
            row_num = req.get("__row_number", "?")
            resource = req.get("ResourceName", "Unknown")[:50]
            player = req.get("DiscordName", "Unknown")[:20]
            position = req.get("QueuePosition", "?")
            remaining = req.get("Remaining", 0)
            total = req.get("Quantity", 0)

            label = f"#{position} {resource}"
            description = f"{player} - {remaining}/{total}"

            options.append(discord.SelectOption(
                label=label[:100],
                description=description[:100],
                value=str(row_num)
            ))

        if options:
            select = Select(
                placeholder="Выберите заявку для управления...",
                options=options,
                min_values=1,
                max_values=1
            )
            select.callback = self.select_callback
            self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        selected_row_num = int(self.children[0].values[0])

        # Find the request
        selected_req = None
        for req in self.requests:
            if req.get("__row_number") == selected_row_num:
                selected_req = req
                break

        if not selected_req:
            await interaction.response.send_message("Заявка не найдена.", ephemeral=True)
            return

        # Create action buttons view
        action_view = RequestActionView(selected_req, self.requests)

        # Update message with selected request details
        embed = discord.Embed(
            title="📋 Управление заявкой",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc)
        )

        resource = selected_req.get("ResourceName", "Unknown")
        player = selected_req.get("DiscordName", "Unknown")
        character = selected_req.get("CharacterName", "Unknown")
        total = selected_req.get("Quantity", 0)
        issued = selected_req.get("IssuedQuantity", 0)
        remaining = selected_req.get("Remaining", total)
        position = selected_req.get("QueuePosition", "?")
        priority = selected_req.get("PriorityLevel", 1)

        embed.add_field(name="Ресурс", value=resource, inline=True)
        embed.add_field(name="Игрок", value=player, inline=True)
        embed.add_field(name="Персонаж", value=character, inline=True)
        embed.add_field(name="Заказано", value=str(total), inline=True)
        embed.add_field(name="Выдано", value=str(issued), inline=True)
        embed.add_field(name="Осталось", value=str(remaining), inline=True)
        embed.add_field(name="Позиция", value=f"#{position}", inline=True)

        if int(priority) > DEFAULT_PRIORITY:
            embed.add_field(name="Приоритет", value=f"👑 Уровень {priority}", inline=True)

        embed.set_footer(text=f"ID заявки: {selected_row_num}")

        await interaction.response.edit_message(embed=embed, view=action_view)


class RequestActionView(View):
    """Buttons for managing a single selected request."""

    def __init__(self, request: Dict[str, Any], all_requests: List[Dict[str, Any]]):
        super().__init__(timeout=300)
        self.request = request
        self.all_requests = all_requests

        # === УЛУЧШЕННОЕ ПОЛУЧЕНИЕ НОМЕРА СТРОКИ ===
        self.row_number = request.get("__row_number")
        if self.row_number is None:
            # Пробуем получить номер строки из других полей
            self.row_number = request.get("RowNumber") or request.get("row_number")

        self.remaining = int(request.get("Remaining", 0) or 0)
        self.issued = int(request.get("IssuedQuantity", 0) or 0)
        self.total = int(request.get("Quantity", 0) or 0)
        self.resource = request.get("ResourceName", "Unknown")
        self.player = request.get("DiscordName", "Unknown")

        # Логирование для отладки
        logger.info(f"RequestActionView создан: row_number={self.row_number}, resource={self.resource}")

    @button(label="➕ Выдать", style=discord.ButtonStyle.primary, row=0)
    async def issue_button(self, interaction: discord.Interaction, button: Button):
        if self.remaining <= 0:
            await interaction.response.send_message("❌ Нечего выдавать - заявка уже выполнена.", ephemeral=True)
            return

        # === ПРОВЕРКА НАЛИЧИЯ НОМЕРА СТРОКИ ===
        if self.row_number is None:
            await interaction.response.send_message(
                "❌ Ошибка: не удалось определить номер строки заявки. Пожалуйста, выберите другую заявку.",
                ephemeral=True
            )
            return

        modal = IssueQuantityModal(
            self.row_number,
            self.resource,
            self.player,
            self.remaining
        )
        await interaction.response.send_modal(modal)

    @button(label="➖ Вернуть", style=discord.ButtonStyle.secondary, row=0)
    async def unissue_button(self, interaction: discord.Interaction, button: Button):
        if self.issued <= 0:
            await interaction.response.send_message("❌ Нечего возвращать.", ephemeral=True)
            return

        # === ПРОВЕРКА НАЛИЧИЯ НОМЕРА СТРОКИ ===
        if self.row_number is None:
            await interaction.response.send_message(
                "❌ Ошибка: не удалось определить номер строки заявки. Пожалуйста, выберите другую заявку.",
                ephemeral=True
            )
            return

        modal = UnissueQuantityModal(
            self.row_number,
            self.resource,
            self.player,
            self.issued
        )
        await interaction.response.send_modal(modal)

    @button(label="✅ Завершить", style=discord.ButtonStyle.green, row=1)
    async def complete_button(self, interaction: discord.Interaction, button: Button):
        try:
            # === ПРОВЕРКА НАЛИЧИЯ НОМЕРА СТРОКИ ===
            if self.row_number is None:
                await interaction.response.send_message(
                    "❌ Ошибка: не удалось определить номер строки заявки.",
                    ephemeral=True
                )
                return

            success = sheets.complete_request(self.row_number)
            if success:
                await interaction.response.send_message(
                    f"✅ Заявка #{self.row_number} завершена!", ephemeral=True
                )

                # Refresh the view
                await self._refresh_queue_view(interaction)
            else:
                await interaction.response.send_message("❌ Ошибка при завершении.", ephemeral=True)
        except Exception as e:
            logger.exception(f"Error completing request: {e}")
            await interaction.response.send_message("❌ Произошла ошибка.", ephemeral=True)

    @button(label="❌ Отменить", style=discord.ButtonStyle.danger, row=1)
    async def cancel_button(self, interaction: discord.Interaction, button: Button):
        # === ПРОВЕРКА НАЛИЧИЯ НОМЕРА СТРОКИ ===
        if self.row_number is None:
            await interaction.response.send_message(
                "❌ Ошибка: не удалось определить номер строки заявки.",
                ephemeral=True
            )
            return

        confirm_view = ConfirmCancelView(self.row_number, self.all_requests)
        await interaction.response.send_message(
            "⚠️ Подтвердите отмену заявки:",
            view=confirm_view,
            ephemeral=True
        )

    @button(label="🔙 Назад к списку", style=discord.ButtonStyle.secondary, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        # Get updated list of requests
        active_requests = sheets.get_active_requests()
        queue_view = QueueManagementView(active_requests)

        embed = discord.Embed(
            title="📋 Управление очередью заявок",
            description="Выберите заявку для управления из списка ниже",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc)
        )

        if active_requests:
            # Группируем по ресурсам для статистики
            resource_stats = {}
            for req in active_requests:
                resource = req.get("ResourceName")
                if resource not in resource_stats:
                    resource_stats[resource] = 0
                resource_stats[resource] += 1

            stats_text = ""
            for resource, count in list(resource_stats.items())[:3]:
                stats_text += f"**{resource}:** {count} заявок\n"

            if len(resource_stats) > 3:
                stats_text += f"... и еще {len(resource_stats) - 3} ресурсов"

            embed.add_field(
                name="📊 Статистика",
                value=stats_text or "Нет данных",
                inline=False
            )

        embed.set_footer(text=f"Всего активных заявок: {len(active_requests)}")

        await interaction.response.edit_message(embed=embed, view=queue_view)

    async def _refresh_queue_view(self, interaction: discord.Interaction):
        """Обновить представление очереди"""
        try:
            # Get updated list of requests
            active_requests = sheets.get_active_requests()
            queue_view = QueueManagementView(active_requests)

            embed = queue_view._create_embed()

            await interaction.response.edit_message(embed=embed, view=queue_view)
        except Exception as e:
            logger.exception(f"Error refreshing queue view: {e}")
            await interaction.response.send_message("❌ Ошибка при обновлении списка.", ephemeral=True)
# ----- Команда для просмотра и управления очередью -----
class QueueManagementView(View):
    """View for managing requests in queue."""

    def __init__(self, requests: List[Dict[str, Any]]):
        super().__init__(timeout=300)
        self.requests = requests

        # Create select dropdown
        options = []
        for i, req in enumerate(requests[:25]):  # Discord limit
            resource = req.get("ResourceName", "Unknown")[:50]
            player = req.get("DiscordName", "Unknown")[:20]
            position = req.get("QueuePosition", "?")
            remaining = req.get("Remaining", 0)
            total = req.get("Quantity", 0)

            label = f"#{position} {resource}"
            description = f"{player} - {remaining}/{total}"

            options.append(discord.SelectOption(
                label=label[:100],
                description=description[:100],
                value=str(i)  # Используем индекс как уникальное значение
            ))

        if options:
            select = Select(
                placeholder="Выберите заявку для управления...",
                options=options,
                min_values=1,
                max_values=1
            )
            select.callback = self.select_callback
            self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        selected_index = int(self.children[0].values[0])

        # Check if index is valid
        if selected_index < 0 or selected_index >= len(self.requests):
            await interaction.response.send_message("Ошибка: неверный индекс заявки.", ephemeral=True)
            return

        # Get the selected request
        selected_req = self.requests[selected_index]

        # Create action buttons view
        action_view = RequestActionView(selected_req, self.requests)

        # Update message with selected request details
        embed = self._create_request_embed(selected_req)

        await interaction.response.edit_message(embed=embed, view=action_view)

    def _create_request_embed(self, req: Dict[str, Any]) -> discord.Embed:
        """Create embed for a single request."""
        embed = discord.Embed(
            title="📋 Управление заявкой",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc)
        )

        resource = req.get("ResourceName", "Unknown")
        player = req.get("DiscordName", "Unknown")
        character = req.get("CharacterName", "Unknown")
        total = req.get("Quantity", 0)
        issued = req.get("IssuedQuantity", 0)
        remaining = req.get("Remaining", total)
        position = req.get("QueuePosition", "?")
        priority = req.get("PriorityLevel", 1)
        row_number = req.get("__row_number", "?")

        embed.add_field(name="Ресурс", value=resource, inline=True)
        embed.add_field(name="Игрок", value=player, inline=True)
        embed.add_field(name="Персонаж", value=character, inline=True)
        embed.add_field(name="Заказано", value=str(total), inline=True)
        embed.add_field(name="Выдано", value=str(issued), inline=True)
        embed.add_field(name="Осталось", value=str(remaining), inline=True)
        embed.add_field(name="Позиция", value=f"#{position}", inline=True)

        if int(priority) > DEFAULT_PRIORITY:
            embed.add_field(name="Приоритет", value=f"👑 Уровень {priority}", inline=True)

        embed.set_footer(text=f"ID строки: {row_number}")

        return embed

    def _create_embed(self) -> discord.Embed:
        """Create embed for the queue list."""
        embed = discord.Embed(
            title="📋 Управление очередью заявок",
            description="Выберите заявку для управления из списка ниже",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc)
        )

        if not self.requests:
            embed.description = "Нет активных заявок для управления."
            return embed

        # Группируем по ресурсам для статистики
        resource_stats = {}
        for req in self.requests:
            resource = req.get("ResourceName")
            if resource not in resource_stats:
                resource_stats[resource] = 0
            resource_stats[resource] += 1

        stats_text = ""
        for resource, count in list(resource_stats.items())[:5]:
            stats_text += f"**{resource}:** {count} заявок\n"

        if len(resource_stats) > 5:
            stats_text += f"... и еще {len(resource_stats) - 5} ресурсов"

        embed.add_field(
            name="📊 Статистика по ресурсам",
            value=stats_text or "Нет данных",
            inline=False
        )

        # Показываем топ-5 ближайших заявок
        if self.requests:
            sorted_requests = sorted(self.requests, key=lambda x: int(x.get("QueuePosition", 999)))
            top_requests = sorted_requests[:5]

            queue_text = ""
            for req in top_requests:
                resource = req.get("ResourceName", "Unknown")
                player = req.get("DiscordName", "Unknown")
                position = req.get("QueuePosition", "?")
                remaining = req.get("Remaining", 0)

                queue_text += f"**#{position}** {resource} - {player} ({remaining} шт.)\n"

            embed.add_field(
                name="🚀 Ближайшие заявки",
                value=queue_text,
                inline=False
            )

        embed.set_footer(text=f"Всего активных заявок: {len(self.requests)}")

        return embed


class ConfirmCancelView(View):
    """Confirmation view for request cancellation."""

    def __init__(self, row_number: int, all_requests: List[Dict[str, Any]]):
        super().__init__(timeout=60)
        self.row_number = row_number
        self.all_requests = all_requests

    @button(label="✅ Да, отменить", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: Button):
        try:
            # === ПРОВЕРКА НАЛИЧИЯ НОМЕРА СТРОКИ ===
            if self.row_number is None:
                await interaction.response.send_message(
                    "❌ Ошибка: не удалось определить номер строки заявки.",
                    ephemeral=True
                )
                return

            # Cancel request via queue manager
            async with sheets_lock:
                queue.cancel_request_by_row(self.row_number, requester_id=interaction.user.id)

            # Get request info for notification
            row = sheets.get_row(self.row_number)
            if row:
                message = f"✅ Заявка #{self.row_number} отменена администратором\n"
                message += f"**Ресурс:** {row.get('ResourceName')}\n"
                message += f"**Игрок:** {row.get('DiscordName')}"

                # Notify player
                player_id = row.get("DiscordID")
                if player_id:
                    try:
                        guild = interaction.guild
                        member = guild.get_member(int(player_id))
                        if member:
                            embed = discord.Embed(
                                title="❌ Ваша заявка отменена",
                                description=f"**{row.get('ResourceName')}**",
                                color=discord.Color.red(),
                                timestamp=datetime.now(timezone.utc)
                            )
                            embed.add_field(name="👮 Отменил", value=interaction.user.display_name, inline=True)
                            embed.add_field(name="📋 Причина", value="Административное решение", inline=True)
                            embed.set_footer(text=f"ID заявки: {row.get('RowID', '')[:8]}")

                            await member.send(embed=embed)
                    except Exception:
                        logger.debug("Could not DM player")
            else:
                message = f"✅ Заявка #{self.row_number} успешно отменена."

            await interaction.response.edit_message(content=message, embed=None, view=None)

            # Refresh queue view in original message
            await self._refresh_original_queue_view(interaction)

        except Exception as e:
            logger.exception(f"Error confirming cancellation: {e}")
            await interaction.response.send_message(
                "❌ Ошибка при отмене заявки.",
                ephemeral=True
            )

    @button(label="❌ Нет, оставить", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.edit_message(
            content="❎ Отмена заявки отменена.",
            embed=None,
            view=None
        )

    async def _refresh_original_queue_view(self, interaction: discord.Interaction):
        """Refresh the original queue view."""
        try:
            # Get the original message
            channel = interaction.channel
            async for message in channel.history(limit=50):
                if message.author == bot.user and message.embeds:
                    if "Управление очередью" in message.embeds[0].title:
                        # Get updated active requests
                        active_requests = sheets.get_active_requests()

                        # Recreate view
                        new_view = QueueManagementView(active_requests)
                        embed = new_view._create_embed()
                        await message.edit(embed=embed, view=new_view)
                        break
        except Exception as e:
            logger.exception(f"Error refreshing original queue view: {e}")
@bot.command(name="очередь")
async def cmd_queue(ctx: commands.Context, resource_name: str = None):
    """Показывает текущую очередь заявок с возможностью управления."""
    try:
        # Check permissions - regular users can only view
        is_admin = ctx.author.guild_permissions.administrator

        init_adapters()
        async with sheets_lock:
            active_requests = sheets.get_active_requests()

        if resource_name:
            # Filter by resource
            active_requests = [
                r for r in active_requests
                if r.get("ResourceName", "").lower() == resource_name.lower()
            ]

        if not active_requests:
            await ctx.send(
                f"📭 Нет активных заявок{f' на ресурс {resource_name}' if resource_name else ''}.",
                ephemeral=True
            )
            return

        # Sort by resource, then by queue position
        active_requests.sort(
            key=lambda x: (
                x.get("ResourceName", ""),
                int(x.get("QueuePosition", 999))
            )
        )

        if is_admin:
            # Show management interface for admins
            view = QueueManagementView(active_requests)
            embed = view._create_embed()
            await ctx.send(embed=embed, view=view, ephemeral=True)
        else:
            # Show read-only interface for regular users
            embed = await _create_readonly_queue_embed(active_requests, resource_name)
            await ctx.send(embed=embed, ephemeral=True)

        # Delete command message after 120 seconds
        await asyncio.sleep(120)
        try:
            await ctx.message.delete()
        except:
            pass

    except Exception as e:
        logger.exception(f"cmd_queue error: {e}")
        await ctx.send("❌ Ошибка при получении информации об очереди.", ephemeral=True)

async def _create_readonly_queue_embed(requests: List[Dict[str, Any]],
                                      resource_filter: str = None) -> discord.Embed:
    """Create read-only embed for queue."""
    embed = discord.Embed(
        title="📋 Текущая очередь заявок",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc)
    )

    # Group by resource
    resources_dict = {}
    for req in requests:
        resource = req.get("ResourceName")
        if resource not in resources_dict:
            resources_dict[resource] = []
        resources_dict[resource].append(req)

    # Sort each resource by queue position
    for resource, reqs in resources_dict.items():
        reqs.sort(key=lambda x: int(x.get("QueuePosition", 999)))

    # Add fields for each resource (limit to 5 resources)
    for resource, reqs in list(resources_dict.items())[:5]:
        queue_text = ""

        for req in reqs[:10]:  # Limit to 10 requests per resource
            player = req.get("DiscordName", "Неизвестно")
            character = req.get("CharacterName", "Неизвестно")
            quantity = req.get("Quantity", "?")
            issued = req.get("IssuedQuantity", 0)
            remaining = req.get("Remaining", quantity)
            pos = req.get("QueuePosition", "?")

            if issued > 0:
                queue_text += f"{pos}. {player} ({character}) - {issued}/{quantity} выдано\n"
            else:
                queue_text += f"{pos}. {player} ({character}) - {quantity} в ожидании\n"

        if len(reqs) > 10:
            queue_text += f"... и еще {len(reqs) - 10} заявок"

        if not queue_text:
            queue_text = "Нет заявок"

        embed.add_field(name=f"**{resource}**", value=queue_text, inline=False)

    if len(resources_dict) > 5:
        embed.set_footer(text=f"Показано 5 из {len(resources_dict)} ресурсов")
    else:
        embed.set_footer(text=f"Всего заявок: {len(requests)}")

    return embed

# Error handlers
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("У вас нет прав для этой команды.", ephemeral=True)
        return
    logger.exception("Command error: %s", error)
    await ctx.send("Произошла ошибка при выполнении команды.", ephemeral=True)

if __name__ == "__main__":
    import sys
    if not getattr(config, "DISCORD_TOKEN", None):
        logger.error("Set DISCORD_TOKEN in config.py")
        sys.exit(1)
    init_adapters()
    bot.run(config.DISCORD_TOKEN)