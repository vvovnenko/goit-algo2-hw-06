import string

from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter

import requests

from matplotlib import pyplot


def get_text(url: str) -> str | None:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        return None


# Функція для видалення знаків пунктуації
def remove_punctuation(text: str) -> str:
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word: str) -> tuple[str, int]:
    return word, 1


def shuffle_function(
    mapped_values: list[tuple[str, int]]
) -> list[tuple[str, list[int]]]:
    shuffled: dict[str, list[int]] = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return list(shuffled.items())


def reduce_function(key_values: tuple[str, list[int]]) -> tuple[str, int]:
    key, values = key_values
    return key, sum(values)


# Виконання MapReduce
def map_reduce(text: str) -> dict[str, int]:
    # Видалення знаків пунктуації
    text = remove_punctuation(text)
    words = text.split()

    # Паралельний Маппінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Крок 2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


def visualize_top_words(result: dict[str, int], n=10):
    top_words = Counter(result).most_common(n)
    words, counts = zip(*top_words)
    pyplot.figure(figsize=(12, 9))
    pyplot.barh(words, counts, color="skyblue")
    pyplot.xlabel("Frequency")
    pyplot.ylabel("Words")
    pyplot.title("Top 10 Most Frequent Words")
    pyplot.gca().invert_yaxis()
    pyplot.show()


if __name__ == "__main__":
    # Вхідний текст для обробки
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)
    if text:
        result = map_reduce(text)

        visualize_top_words(result)
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")
