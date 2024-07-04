def convert_abbreviated_number(abbrev_number):
    abbrev_number = abbrev_number.lower()
    multipliers = {'k': 1000, 'm': 1000000, 'b': 1000000000}
    if abbrev_number[-1] in multipliers:
        multiplier = multipliers[abbrev_number[-1]]
        return int(float(abbrev_number[:-1]) * multiplier)
    else:
        return int(abbrev_number)


def get_gpt_payload(prompt):
    messages = [
        {
            "role": "user",
            "content": prompt
        }
    ]

    data = {
        'model': 'gpt-3.5-turbo',
        'messages': messages,
        'max_tokens': 250
    }

    return data


def get_rank(s):
    try:
        return int(s)
    except ValueError:
        return 0
