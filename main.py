"""
Точка входа олимпиадного бота.

- app.config       — настройки из .env
- app.database     — SQLite (пользователи, подписки)
- app.excel_data   — чтение списка олимпиад из Excel
- app.dates        — разбор дат из ячеек
- app.keyboards    — инлайн-клавиатуры
- app.ui           — безопасное редактирование сообщений, чанкинг текста
- app.reminders    — построение и рассылка ежедневных напоминаний
- app.handlers     — обработчики команд и колбэков
"""
import logging

from telegram.error import Conflict
from telegram.ext import Application, ApplicationBuilder

from app import config
from app.database import init_db
from app.handlers import register_handlers
from app.reminders import fallback_daily_scheduler, send_daily


async def _post_init(app: Application):
    """Запускаем fallback-планировщик, если нет JobQueue (не установлен python-telegram-bot[job-queue])."""
    if getattr(app, "job_queue", None) is None:
        app.create_task(fallback_daily_scheduler(app, config.NOTIFY_TIME))
        logging.warning("JobQueue не найден — используется fallback-планировщик.")
    else:
        logging.info("JobQueue доступен — используется стандартный планировщик.")


def main():
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    if not config.TELEGRAM_TOKEN:
        raise SystemExit(
            "TELEGRAM_TOKEN не задан. Скопируйте .env.example в .env и укажите токен от @BotFather."
        )
    init_db()
    app = ApplicationBuilder().token(config.TELEGRAM_TOKEN).post_init(_post_init).build()
    register_handlers(app)
    if getattr(app, "job_queue", None) is not None:
        app.job_queue.run_daily(send_daily, time=config.NOTIFY_TIME, name="send_daily_job")
    try:
        app.run_polling(drop_pending_updates=True)
    except Conflict:
        logging.error("Запуск не удался: другой экземпляр бота уже запущен.")


if __name__ == "__main__":
    main()
