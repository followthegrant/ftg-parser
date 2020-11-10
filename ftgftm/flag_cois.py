from normality import normalize


def get_has_conflict(text):
    """flag potential conflict based on splitted coi (individual per author)"""
    text = normalize(text.replace('\n', ' '))
    if not text:
        return False
    if 'kein interessenkonflikt besteht' in text:
        return False
    if 'no conflict' in text:
        return False
    if 'no potential conflict' in text:
        return False
    if 'no competing' in text:
        return False
    if 'no financial' in text:
        return False
    if 'no known' in text:
        return False
    # if 'die ubrigen' in text:
    #    return True
    # if 'all others' in text:
    #    return True
    # if 'remaining authors' in text:
    #    return True
    # if 'no other' in text:
    #    return True
    if 'employee' in text:
        return True
    # if 'funded by':
    #    return True
    if 'research grant' in text:
        return True
    if text == 'none':
        return False
    if text.startswith('none '):
        return False
    if text.endswith('none'):
        return False
    if 'nothing to report' in text:
        return False
    if 'none declared' in text:
        return False
    if 'no authors have' in text:
        return False
    if 'none of the authors have' in text:
        return False
    if len(text) < 110:
        return False
    return True


def flag_coi(text):
    return get_has_conflict(text)
