# StashKeeper — Discord бот для распределения дропа

Коротко: бот принимает заявки на ресурсы, поддерживает приоритеты, для **фиолетовых** ресурсов требует скриншот рецепта, хранит заявки в Google Sheets и скриншоты в Google Drive.

---

## Что в репо
- `bot.py` — основной модуль бота (UI: Select + Modal)
- `sheets_adapter.py` — обёртка для Google Sheets (должна быть в проекте)
- `drive_uploader.py` — загрузка скриншотов в Google Drive (должна быть в проекте)
- `queue_manager.py` — бизнес-логика очереди (должна быть в проекте)
- `config.py` — локальный конфиг (НЕ коммитить в git)
- `credentials_template.json` — сервисный аккаунт (НЕ коммитить)

---

## Требования
Python 3.10+

Установить зависимости:
```bash
python -m venv venv
source venv/bin/activate    # Linux / macOS
venv\Scripts\activate       # Windows
pip install -r requirements.txt
