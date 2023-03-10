import re
from collections import Counter, defaultdict

from html2text import html2text
from lxml import etree
from normality import collapse_spaces, normalize
from spacy.lang.en import English

nlp = English()
nlp.add_pipe("sentencizer")


def _get_author_tests(author):
    first_names, last_name = author
    name = " ".join((first_names, last_name))
    first_name = re.split(r"[\W]+", first_names)[0]
    initials = list(_get_initials((first_names, last_name)))

    # 1. Paula Theda Anderson
    yield r"(^|\W){name}\W".format(name=name)

    # 2. Paula Anderson
    yield r"(^|\W){name}\W".format(name=" ".join((first_name, last_name)))

    # 3. PTA / P.T.A / P-TA / P.-T.A. / and all again with whitespace
    initials_re = "".join(r"{i}[\s\.-]*".format(i=i) for i in initials)
    yield r"(^|\W){re}\W".format(re=initials_re)

    # 4. PA / P.A. / and all again with whitespace
    yield r"(^|\W){fi}[\s\.-]*{li}\W".format(fi=initials[0], li=initials[-1])

    # 5. PT Anderson / P.T. Anderson / P.-T. Anderson / and all with whitespace
    initials_re = "".join(r"{i}[\s\.-]*".format(i=i) for i in initials[:-1])
    yield r"(^|\W){re}{last_name}\W".format(re=initials_re, last_name=last_name)

    # 6. P Anderson / P. Anderson
    yield r"(^|\W){i}[\s\.]*{last_name}\W".format(i=initials[0], last_name=last_name)

    # 7. Paula T. Anderson / Paula T Anderson
    if len(initials) > 2:
        initials_re = "".join(r"{i}[\s\.-]+".format(i=i) for i in initials[1:-1])
        yield r"(^|\W){first_name}\W{ire}{last_name}\W".format(
            first_name=first_name, ire=initials_re, last_name=last_name
        )


def _test_sentence(sentence, tests):
    for test in tests:
        if re.search(test, sentence, re.IGNORECASE):
            return True


def _get_initials(author):
    # author: ("Firstname", "Lastname)
    for part in author:
        for name in re.split(r"\W", part):
            if name:
                yield name[0]


def split_coi(coi_text, authors):
    """
    authors: [('FirstName', 'LastName'), ('FirstName MiddleName', 'LastName')]

    return: (author1, [sentences]), (author2, ...)

    1. First test for fullname (Paula Theda Anderson)
    2. Then for first / last name (Paula Anderson)
    3. Then for all the initials (P.T.A.)
    4. Then for first and last initials (P.A.)
    5. Then for the first/middle name initial and last name (P. T. Anderson)
    6. Then for the first initial and the last name (P. Anderson)
    7. Then for the first name, middle name initials and last name (Paula T. Anderson)
    8. If the lastname is unique look for it
    9. Look for the remaining authors
    """
    coi_text = " ".join(coi_text.split())
    sentences = (  # we need a generator here
        s
        for sentence in nlp(coi_text).sents
        for s in re.split(
            r"(?<=Ltd.\W)|(?<=Inc.\W)|(?<=Drs. \W)|(?<=Dr. \W)|(?<=Prof. \W)(?=[A-Z])",
            sentence.text,
        )
    )
    surnames = Counter()
    author_tests = defaultdict(list)

    # compute search & test data for each author to apply for each sentence later
    for first_names, last_name in authors:
        surnames[last_name] += 1
        author_tests[(first_names, last_name)] = list(
            _get_author_tests((first_names, last_name))
        )

    # 8. unique last name
    unique_surnames = [s for s, i in surnames.items() if i == 1]

    # 9. "remaining authors"
    remaining_tests = (
        r"(^|\W)(all|the)\s+(other|remaining)\s+(co-)?authors?\W",
        r"(^|\W)(alle|die)\s+(anderen|??brigen)(\s+autoren)?\W",  # de
    )

    found_authors = set()

    def _parse_sentence(sentence, current_authors=set()):
        found = False
        for author, tests in author_tests.items():
            if author[-1] in unique_surnames:
                tests = tests + [r"(^|\W){a}\W".format(a=author[-1])]
            if _test_sentence(sentence, tests):
                current_authors.add(author)
                found_authors.add(author)
                found = True
                yield author, sentence

        if found:
            # continue next sentence with current authors:
            try:
                yield from _parse_sentence(next(sentences), current_authors)
            except StopIteration:  # no more sentences
                return

        if not found:
            # test for "remaining authors"
            if _test_sentence(sentence, remaining_tests):
                # clear current authors
                current_authors.clear()
                for author in author_tests:
                    if author not in found_authors:
                        yield author, sentence

            # assign to current authors (from last sentence)
            elif len(current_authors):
                for author in current_authors:
                    yield author, sentence

            # or assign sentence to all authors
            else:
                for author in author_tests:
                    yield author, sentence

    res = defaultdict(list)
    for sentence in sentences:
        for author, s in _parse_sentence(sentence):
            res[author].append(s)

    # return result
    return res


def flag_coi(text):
    """flag potential conflict based on splitted coi (individual per author)"""
    text = normalize(text.replace("\n", " "))
    if not text:
        return False
    if "kein interessenkonflikt besteht" in text:
        return False
    if "no conflict" in text:
        return False
    if "no potential conflict" in text:
        return False
    if "no competing" in text:
        return False
    if "no financial" in text:
        return False
    if "no known" in text:
        return False
    # if 'die ubrigen' in text:
    #    return True
    # if 'all others' in text:
    #    return True
    # if 'remaining authors' in text:
    #    return True
    # if 'no other' in text:
    #    return True
    if "employee" in text:
        return True
    # if 'funded by':
    #    return True
    if "research grant" in text:
        return True
    if text == "none":
        return False
    if text.startswith("none "):
        return False
    if text.endswith("none"):
        return False
    if "nothing to report" in text:
        return False
    if "none declared" in text:
        return False
    if "no authors have" in text:
        return False
    if "none of the authors have" in text:
        return False
    if len(text) < 110:
        return False
    return True


# Stylianos Serghiou implementation
FLAG_COI_NEGATIVE_PATTERNS = (
    re.compile(r"[Nn]o (|potential )(conflict(|s)|competing)"),
    re.compile(r"[Nn]o (|potential )(commercial|financial)"),
    re.compile(r"(disclose|declare|report) (|that they have )(no\s|nothing|none)"),
    re.compile(r"None of the"),
    re.compile(r"(None|Nothing|Nil\s) (declared|disclosed)"),
    re.compile(r"Nothing to (declare|disclose|report)"),
)


def flag_coi_rtrans(text):
    """
    implementation by Stylianos Serghiou

    applicable for unsplitted cois

    https://www.biorxiv.org/content/10.1101/2020.10.30.361618v1

    https://github.com/serghiou/transparency-indicators/blob/master/4_automated-evaluation/code/tidy_code/indicator-eval.Rmd#L783
    """

    text = text.strip().strip('"')

    if text.lower().startswith("acknowled"):
        try:
            text = re.split(r"[Ss]tatement|[Cc]ompeting\s+interests", text)[1]
        except IndexError:
            return False

    text = re.split("[Aa]cknowledge?ments", text)[0]
    if not text:
        return False

    for pat in FLAG_COI_NEGATIVE_PATTERNS:
        if re.search(pat, text):
            return False

    return True


def flag_coi_hristio(text):
    """
    Hristios implementation:
    https://docs.google.com/document/d/1_-Tb-1IvTKBgRetOE9Ih4fYupsjsYgzjo1f_I5t_IWY/edit
    """
    text = normalize(text) or ""
    if re.search(
        r"(the|all)\s+authors?\s+have\s+(declared\s+no|nothing\s+to\s+declare)", text
    ):
        return False

    for test in (
        "consulting",
        "consultant",
        "payment",
        "payments",
        "contract",
        "contracts",
        "fees",
        "employee",
        "employed",
        "advisory board",
        "advisory boards",
        "advisor",
        "royalties",
        "royalty",
        "patent",
        "patents",
        "honoraria",
        "committee",
        "committees",
        "company",
        "companies",
        "collaborator",
        "founder",
        "founded",
        "all others",
        "all other authors",
        "remaining authors",
        "no other",
        "funders played",
    ):
        if f"{test} " in text and f"no {test} " not in text:
            return True

    for test in (
        "no conflict",
        "no potential conflict",
        "no competing",
        "no financial",
        "no known",
        "nothing to report",
        "none declared",
        "no authors have",
        "none of the authors have",
    ):
        if test in text:
            return False

    for test in ("received", "receives", "recipient"):
        if f" {test} " in text:
            return True

    return False


def extract_coi_from_fulltext(fpath):
    from .parse.xml import read_xml

    tree = read_xml(fpath)
    xpath = './/*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"interest") and (contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"competing") or contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"declaring") or contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"conflict"))]'  # noqa
    coi_statement = " ".join(
        " ".join(t for t in el.itertext()) for el in tree.xpath(xpath)
    )
    if len(coi_statement) > 36:
        return coi_statement
    if coi_statement:
        if isinstance(
            fpath, etree._Element
        ):  # FIXME make xml loading consistent everywhere
            article_text = etree.tostring(fpath).decode()
        else:
            with open(fpath) as f:
                article_text = f.read()
        article_text = html2text(article_text)
        coi_statement = coi_statement.replace("(", "\(").replace(")", "\)")  # noqa
        match = re.search(coi_statement, article_text, flags=re.IGNORECASE)
        if match is not None:
            start_pos = match.start()
            full_coi_text = ""
            i = 0
            while True:
                char = article_text[start_pos + i]
                try:
                    if (
                        char == "\n"
                        and article_text[start_pos + i + 1] == "\n"
                        and i > len(coi_statement)
                    ):
                        break
                except IndexError:  # end of text
                    break
                else:
                    full_coi_text += article_text[start_pos + i]
                    i += 1

            return collapse_spaces(full_coi_text)
