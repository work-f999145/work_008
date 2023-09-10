import psutil
import time


def occupied_memory():
    # Получить список всех процессов
    all_processes = psutil.process_iter()

    # Пройтись по каждому процессу и найти процессы с именем "chrome.exe"
    chrome_processes = [p for p in all_processes if p.name() == "chrome.exe"]

    # Инициализировать переменную для хранения общего объема памяти
    total_memory = 0

    # Получить информацию о памяти для каждого процесса Chrome

    if chrome_processes:
        for process in chrome_processes:
            try:
                memory_info = process.memory_info()
            except psutil.NoSuchProcess:
                memory_usage = 0
            else:
                memory_usage = memory_info.rss
                total_memory += memory_usage

    # Преобразовать размер в удобочитаемый формат
    # formatted_size = psutil._common.bytes2human(total_memory)
    return (total_memory) / (1024**2)


# Бесконечный цикл
while True:
    # Ваш код для получения информации, которую вы хотите вывести
    data = occupied_memory()

    # Вывести информацию с использованием символа возврата каретки
    print(f"\rChrome: {data:5,.0f} MB", end='', flush=True)

    # Добавить символ перезаписи строки для очистки оставшихся символов
    print("\033[K", end='', flush=True)

    # Задержка, чтобы обновление было заметно
    time.sleep(1)