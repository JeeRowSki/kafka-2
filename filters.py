import re


def normalize_word(word: str) -> str:
    return word.strip().casefold()


def censor_text(text: str, banned_words: set[str]) -> str:
    result = text

    for word in sorted(banned_words, key=len, reverse=True):
        word = normalize_word(word)

        if not word:
            continue

        pattern = re.compile(
            rf"(?<!\w){re.escape(word)}(?!\w)",
            flags=re.IGNORECASE,
        )

        result = pattern.sub(
            lambda match: "*" * len(match.group()),
            result,
        )

    return result
