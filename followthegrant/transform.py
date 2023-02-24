"""
generate entities from parsed result
"""

from collections import defaultdict
from typing import Generator, Iterable

from banal import ensure_dict

from .exceptions import TransformException
from .ftm import EGenerator, SKDict
from .model import (
    CE,
    Affiliation,
    Article,
    Author,
    BaseModel,
    Documentation,
    Employment,
    Grant,
    Institution,
    Journal,
    ParsedResult,
    ProjectParticipant,
    Statement,
)
from .ner import analyze


def get_article_context(context: SKDict) -> SKDict:
    context.pop("summary", None)
    context.pop("description", None)
    return context


def merge_authors(authors: list[Author]) -> list[Author]:
    """
    within 1 article scope, we assume authors with the same fingerprint are the same.
    this helps to collect name mentions of authors inside e.g. the pubmed xml,
    where the grant participants elements don't have orcid but the main author
    element has
    """
    merged_authors = []
    authors_dict: dict[str, list[Author]] = defaultdict(list)
    for author in authors:
        authors_dict[author.fingerprint].append(author)
    for _, authors in authors_dict.items():
        base, *others = authors
        for o in others:
            base = base.merge(o)
        merged_authors.append(base)
    return merged_authors


def apply_institution_addresses(
    authors: list[Author], institutions: list[Institution] | None
) -> list[Author]:
    if not institutions:
        return authors
    for institution in institutions:
        for author in authors:
            if author.xref_affiliation & institution.xref:
                author.address.update(institution.name)
                author.address.update(institution.address)
            if author.xref_employment & institution.xref:
                author.address.update(institution.name)
                author.address.update(institution.address)
    return authors


def make_publication(
    journal: Journal, article: Article
) -> Generator[Documentation, None, None]:
    context = get_article_context(article.properties)
    yield Documentation(
        document=article.id,
        entity=journal.id,
        role="PUBLISHER",
        **context,
    )


def make_affiliations(
    institutions: Iterable[Institution], authors: Iterable[Author], **context
) -> Generator[Affiliation, None, None]:
    for institution in institutions:
        for author in authors:
            if author.xref_affiliation & institution.xref:
                yield Affiliation(
                    organization=institution.id,
                    member=author.id,
                    role="AFFILIATION",
                    **context,
                )
            if author.xref_employment & institution.xref:
                yield Employment(
                    employer=institution.id,
                    employee=author.id,
                    role="EMPLOYMENT",
                    **context,
                )


def make_authorships(
    article: Article, authors: Iterable[Author]
) -> Generator[Documentation, None, None]:
    context = get_article_context(article.properties)
    for author in authors:
        yield Documentation(
            document=article.id, entity=author.id, role="AUTHORSHIP", **context
        )


def make_grant_relations(
    grants: Iterable[Grant],
    authors: Iterable[Author],
    institutions: Iterable[Institution],
    article: Article | None = None,
    **context
):
    for grant in grants:
        context = {**grant.properties, **context}
        context.pop("summary", None)
        context.pop("description", None)
        if article is not None:
            context = {**get_article_context(article.properties), **context}
            yield Documentation(
                entity=grant.id,
                document=article.id,
                role="GRANT",
                **context,
            )

        for author in authors:
            if author.xref_grant_recipient & grant.xref:
                yield ProjectParticipant(
                    project=grant.id,
                    participant=author.id,
                    role="PARTICIPANT",
                    **context,
                )
            if author.xref_grant_investigator & grant.xref:
                yield ProjectParticipant(
                    project=grant.id,
                    participant=author.id,
                    role="INVESTIGATOR",
                    **context,
                )

        for institution in institutions:
            if institution.xref_grant_funder & grant.xref:
                yield ProjectParticipant(
                    project=grant.id,
                    participant=institution.id,
                    role="FUNDER",
                    **context,
                )


def make_funding_relations(
    institutions: Iterable[Institution],
    article: Article,
) -> EGenerator:
    context = get_article_context(article.properties)
    for institution in institutions:
        if article.xref_funding & institution.xref:
            yield Documentation(
                entity=institution.id,
                document=article.id,
                role="FUNDER",
                **context,
            )


def to_proxies(items: Generator[BaseModel, None, None]) -> EGenerator:
    for item in items:
        yield item.proxy


def _make_proxies(data: ParsedResult) -> EGenerator:
    """
    this looks a bit weird but it allows arbitrary parsing from different
    sources (e.g., sometimes with articles, sometimes only authors with or
    without institutions and so on...) and improving context / adjacent data &
    entities during the process
    """
    context = get_article_context(ensure_dict(data.article))
    if data.journal:
        data.journal = Journal(**data.journal)
        yield data.journal.proxy
    if data.institutions:
        data.institutions = [Institution(**i) for i in data.institutions]
        yield from to_proxies(data.institutions)
    if data.authors:
        data.authors = [Author(**i) for i in data.authors]
        data.authors = apply_institution_addresses(data.authors, data.institutions)
        data.authors = merge_authors(data.authors)
        yield from to_proxies(data.authors)
    if data.grants:
        data.grants = [Grant(**i) for i in data.grants]
        yield from to_proxies(data.grants)
    if data.article:
        data.article = Article(
            data,
            **{**data.article, **{"journal": data.journal, "authors": data.authors}},
        )
        yield data.article.proxy
        # improve context for later use
        context = get_article_context(data.article.properties)

    # relations
    if data.journal and data.article:
        yield from to_proxies(make_publication(data.journal, data.article))
    if data.article and data.authors:
        yield from to_proxies(make_authorships(data.article, data.authors))
    if data.institutions and data.authors:
        yield from to_proxies(
            make_affiliations(data.institutions, data.authors, **context)
        )
    if data.grants and (data.authors or data.institutions):
        yield from to_proxies(
            make_grant_relations(
                data.grants, data.authors, data.institutions, data.article, **context
            )
        )
    if data.institutions and data.article:
        yield from to_proxies(make_funding_relations(data.institutions, data.article))

    # statements
    if data.article:
        for key in ("coi_statement", "ack_statement", "funding_statement"):
            stmt = getattr(data.article, key)
            if stmt:
                stmt = Statement(data, bodyText=stmt, title=key.upper())
                yield stmt.proxy
                yield from to_proxies(stmt.get_documentations())


def make_proxies(data: ParsedResult, dataset: str | None = None) -> EGenerator:
    proxies: dict[str, CE] = dict()
    try:
        for proxy in _make_proxies(data):
            for proxy in analyze(proxy):
                if dataset is not None:
                    proxy.datasets.add(dataset)
                if proxy.id in proxies:
                    proxies[proxy.id].merge(proxy)
                else:
                    proxies[proxy.id] = proxy
        yield from proxies.values()
    except Exception as e:
        raise TransformException(e)
