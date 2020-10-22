import re
from collections import defaultdict
from spacy.lang.en import English

nlp = English()
sentencizer = nlp.create_pipe("sentencizer")
nlp.add_pipe(sentencizer)


def split_coi(coi_text, authors):
    coi_splitted = defaultdict(list)
    sentences = nlp(coi_text).sents

    authors_search = {}
    authors_search_name = {}

    def get_initials(name):
        if isinstance(name, str):
            return [name_part[0] for name_part in re.split(r'\W', name) if len(name_part) > 0]
        else:
            return ['']

    unique_surname = {}
    for a in authors:
        unique_surname[a[1]] = len([a_sur[1] for a_sur in authors if a_sur == a[1]]) > 1
        initials = (get_initials(a[0]),  # First and middle name initials
                    get_initials(a[1]))  # Lastname initial(s)
        authors_search_name[a] = (re.split(r'\W', str(a[0])), re.split(r'\W', str(a[1])))
        authors_search[a] = initials

    last_author_found = None
    sentences = [sent_split for s in sentences for sent_split in re.split(r'(?<=Ltd.\W)|(?<=Inc.\W)(?=[A-Z])', s.text)]
    for s in sentences:
        found_an_author = False
        in_all = False
        in_remaining = False
        for a_name, a_initials in authors_search.items():
            # 1. First test for fullname (Paul Thomas Anderson)
            # 2. Then for all the initials (P.T.A.)
            # 3. Then for the first/middle name initial and last name (P. T. Anderson)
            # 4. Then for the first initial and the last name (P. Anderson)
            # 5. Then for the first name, middle name initials and last name (Paul T. Anderson)
            # 6. If the lastname is unique look for it
            # 7. Look for the remaining authors
            if str(a_name[0]) + ' ' + str(a_name[1]) in s or \
               re.match(r'.*[\W]*' + ''.join(['{}[\.-]*'.format(i) for i in a_initials[0] + a_initials[1]]) + '\W', s) or \
               re.match(r'.*' + ''.join(['{}[\.-]*'.format(i) for i in a_initials[0]]) + '\W{}'.format(a_name[1]), s) or \
               re.match(r'.*{}[\.-]*'.format(a_initials[0][0]) + '\W{}'.format(a_name[1]), s) or \
               re.match(r'.*{}'.format(authors_search_name[a_name][0][0]) + ''.join(['[{}]*[\.-]*'.format(i) for i in a_initials[0][1:]]) + '\W{}'.format(a_name[1]), s) or \
               a_name[1] in s and unique_surname[a_name[1]]:
                coi_splitted[a_name].append(s)
                found_an_author = True
                last_author_found = a_name

        if not found_an_author:
            if 'll authors' in s or 'he authors' in s:
                coi_splitted['all'].append(s)
                in_all = True

        if 'remaining author' in s:
            in_remaining = True
            for a in [a for a in authors_search.keys() if a not in coi_splitted.keys()]:
                coi_splitted[a].append(s)

        if not found_an_author and not in_all and not in_remaining:
            if last_author_found is not None:
                coi_splitted[last_author_found].append(s)
            else:
                coi_splitted['all'].append(s)

    for a, c in coi_splitted.items():
        yield a, c
