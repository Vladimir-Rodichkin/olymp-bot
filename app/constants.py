"""Ключи."""

# Сообщения-списки («Мои подписки»), которые нужно чистить при возврате в меню
UD_LIST_ROOT_ID = "list_root_msg_id"
UD_LIST_EXTRA_IDS = "list_message_ids"

# Активное сообщение с меню/диалогом (чтобы не плодить дубликаты при /start)
UD_ACTIVE_MSG_ID = "active_msg_id"

# Ожидание текста рассылки от админа
UD_AWAIT_BROADCAST = "await_broadcast_text"

# Состояние сценария подписки
UD_OLYS = "olys"                     # список олимпиад из Excel, снятый на момент открытия меню
UD_SELECTION = "selection"           # выбранные профили (чекбоксы)
UD_CHOSEN = "chosen"                 # итоговый список [(olympiad, profile), ...] для сохранения
UD_PROFILES = "profiles"             # список всех профилей (для сопоставления индекс -> профиль)
UD_PROFILE_LIST = "profile_list"     # выбранные профили, по которым идёт пошаговый обход
UD_CURRENT_PROFILE = "current_profile"
UD_MANUAL_LIST = "manual_list"       # олимпиады текущего профиля для ручного выбора
UD_MANUAL_SEL = "manual_sel"         # выбранные индексы при ручном выборе

# Состояние сценария удаления
UD_REMOVE = "remove"                 # список подписок пользователя для удаления одной
UD_DEL_PROFILES = "del_profiles"     # список профилей пользователя для удаления по профилю
