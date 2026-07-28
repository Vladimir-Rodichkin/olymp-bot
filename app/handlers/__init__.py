"""Регистрация всех хендлеров бота в Application."""
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from app.handlers import admin, delete, fallback, menu, subscribe


def register_handlers(app: Application) -> None:
    # Меню
    app.add_handler(CommandHandler("start", menu.start))
    app.add_handler(CallbackQueryHandler(menu.menu_back_cb, pattern="^menu_back$"))
    app.add_handler(CallbackQueryHandler(menu.menu_select_cb, pattern="^menu_select$"))
    app.add_handler(CallbackQueryHandler(menu.menu_list_cb, pattern="^menu_list$"))
    app.add_handler(CallbackQueryHandler(menu.menu_delete_cb, pattern="^menu_delete$"))

    # Удаление
    app.add_handler(CallbackQueryHandler(delete.del_one_cb, pattern="^del_one$"))
    app.add_handler(CallbackQueryHandler(delete.del_one_oly_cb, pattern=r"^del_one_oly\|"))
    app.add_handler(CallbackQueryHandler(delete.del_profile_cb, pattern="^del_profile$"))
    app.add_handler(CallbackQueryHandler(delete.del_profile_sel_cb, pattern=r"^del_profile_sel\|"))

    # Подписка
    app.add_handler(CallbackQueryHandler(subscribe.toggle_profile_cb, pattern=r"^toggle_profile\|"))
    app.add_handler(CallbackQueryHandler(subscribe.profiles_done_cb, pattern="^profiles_done$"))
    app.add_handler(CallbackQueryHandler(subscribe.include_all_cb, pattern="^include_all$"))
    app.add_handler(CallbackQueryHandler(subscribe.include_manual_cb, pattern="^include_manual$"))
    app.add_handler(CallbackQueryHandler(subscribe.toggle_oly_cb, pattern=r"^toggle_oly\|"))
    app.add_handler(CallbackQueryHandler(subscribe.manual_done_cb, pattern="^manual_done$"))

    # Админ
    app.add_handler(CommandHandler("broadcast", admin.broadcast_cmd))
    app.add_handler(CommandHandler("testnotify", admin.test_notify_cmd))

    # Текст и неизвестные команды
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback.catch_all))
    app.add_handler(MessageHandler(filters.COMMAND, fallback.unknown_command))
    app.add_error_handler(fallback.error_handler)
