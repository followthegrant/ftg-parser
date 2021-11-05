from ftg.model import Article
from ftg.schema import ArticleFullOutput


def parse_article(data) -> ArticleFullOutput:
    """
    data must be preprocessed by any of the processors in `preprocess.py`
    """
    article = Article(**data)
    data = article.serialize()
    if article.identifiers:
        data["identifiers"] = [i.serialize() for i in article.identifiers]
    if article.coi_statement:
        data["coi_statement"] = article.coi_statement.serialize()
        data["individual_coi_statements"] = [
            s.serialize() for s in article.individual_coi_statements
        ]

    return ArticleFullOutput(**data)
