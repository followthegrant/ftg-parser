from typing import Iterator, Optional

from followthemoney.util import sanitize_text

from .schema import (
    ArticleFtm,
    ArticleFullOutput,
    ArticleIdentifierFtm,
    ArticlePublishedFtm,
    AuthorFtm,
    AuthorshipFtm,
    CoiStatementFtm,
    AckStatementFtm,
    MembershipFtm,
    OrganizationFtm,
    PublisherFtm,
)
from .util import cached_property, prefixed_dict


def ftm_dict(data: dict, prefix: Optional[str] = None, context: Optional[dict] = {}):
    data = prefixed_dict(data, prefix)
    data.update(context)
    return {k: sanitize_text(v) for k, v in data.items()}


class MappedModel:
    def __init__(self, instance: ArticleFullOutput):
        self.input = instance
        self.context = {
            "journal_id": instance.journal.id,
            "journal_name": instance.journal.name,
            "article_id": instance.id,
            "article_published_at": str(instance.published_at),
        }

    def __iter__(self):
        yield self.publisher
        yield self.article
        yield self.articlepublished
        yield from self.identifiers
        yield from self.authors
        yield from self.authorships
        yield from self.organizations
        yield from self.memberships
        yield from self.coi_statements
        yield from self.ack_statements

    @cached_property
    def publisher(self) -> PublisherFtm:
        data = ftm_dict(self.input.journal.dict(), "journal")
        return PublisherFtm(**data)

    @cached_property
    def article(self) -> ArticleFtm:
        data = ftm_dict(self.input.dict(), "article", self.context)
        data["article_authors"] = ",".join([a.name for a in self.input.authors])
        return ArticleFtm(**data)

    @cached_property
    def articlepublished(self) -> ArticlePublishedFtm:
        return ArticlePublishedFtm(**self.context)

    @cached_property
    def identifiers(self) -> Iterator[ArticleIdentifierFtm]:
        for identifier in self.input.identifiers:
            data = ftm_dict(identifier.dict(), "articleidentifier", self.context)
            yield ArticleIdentifierFtm(**data)

    @cached_property
    def authors(self) -> Iterator[AuthorFtm]:
        for author in self.input.authors:
            data = author.dict()
            data["countries"] = ",".join(data["countries"])
            data = ftm_dict(data, "author", self.context)
            yield AuthorFtm(**data)

    @cached_property
    def authorships(self) -> Iterator[AuthorshipFtm]:
        for author in self.input.authors:
            data = {"author_id": author.id}
            data.update(self.context)
            yield AuthorshipFtm(**data)

    @cached_property
    def organizations(self) -> Iterator[OrganizationFtm]:
        for author in self.input.authors:
            for institution in author.institutions:
                data = ftm_dict(institution.dict(), "institution", self.context)
                yield OrganizationFtm(**data)

    @cached_property
    def memberships(self) -> Iterator[MembershipFtm]:
        for author in self.input.authors:
            for institution in author.institutions:
                data = {
                    "author_id": author.id,
                    "institution_id": institution.id,
                }
                data.update(self.context)
                yield MembershipFtm(**data)

    @cached_property
    def coi_statements(self) -> Iterator[CoiStatementFtm]:
        if self.input.coi_statement is not None:
            data = ftm_dict(self.input.coi_statement.dict(), "coi")
            data["coi_authors"] = ",".join([a.name for a in self.input.authors])
            yield CoiStatementFtm(**data)
            for statement in self.input.individual_coi_statements:
                data = ftm_dict(statement.dict(), "coi")
                yield CoiStatementFtm(**data)

    @cached_property
    def ack_statements(self) -> Iterator[AckStatementFtm]:
        if self.input.ack_statement is not None:
            data = ftm_dict(self.input.ack_statement.dict(), "ack")
            data["ack_authors"] = ",".join([a.name for a in self.input.authors])
            yield AckStatementFtm(**data)
            for statement in self.input.individual_ack_statements:
                data = ftm_dict(statement.dict(), "ack")
                yield AckStatementFtm(**data)
