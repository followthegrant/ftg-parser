from datetime import date
from typing import List, Optional, Union

from pydantic import BaseModel


class JournalInput(BaseModel):
    identifier: Optional[str] = None
    name: str


class JournalOutput(BaseModel):
    id: str
    name: str


class InstitutionInput(BaseModel):
    identifier: Optional[str] = None
    name: str
    country: Optional[str] = None


class InstitutionOutput(BaseModel):
    id: str
    name: str
    country: Optional[str] = None


class ArticleIdentifierInput(BaseModel):
    key: str
    value: str


class ArticleIdentifierOutput(BaseModel):
    id: str
    key: str
    label: str
    value: str


class AuthorInput(BaseModel):
    name: str
    identifier: Optional[str] = None
    identifier_hints: Optional[List[str]] = []
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    middle_names: Optional[str] = None
    institutions: Optional[List[InstitutionInput]] = []


class AuthorOutput(BaseModel):
    id: str
    name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    middle_names: Optional[str] = None
    institutions: Optional[List[InstitutionOutput]] = []
    countries: Optional[List[str]] = []


class ArticleInput(BaseModel):
    journal: JournalInput
    identifiers: Optional[dict] = {}
    title: str
    published_at: Union[date, str]
    abstract: Optional[str] = None
    keywords: Optional[list] = []
    authors: Optional[list[AuthorInput]] = []
    coi_statement: Optional[str] = None


class ArticleOutput(BaseModel):
    id: str
    title: str
    published_at: date
    abstract: Optional[str] = None
    journal: JournalOutput
    authors: Optional[List[AuthorOutput]] = []
    index_text: Optional[str] = None


class CoiStatementInput(BaseModel):
    text: str
    article_id: str
    article_title: Optional[str]
    article: Optional[ArticleOutput]
    author_id: Optional[str]
    author_name: Optional[str]
    author: Optional[AuthorOutput]
    published_at: Optional[date]
    journal_name: Optional[str]


class CoiStatementOutput(BaseModel):
    id: str
    article_id: str
    article_title: Optional[str]
    author_id: Optional[str]
    author_name: Optional[str]
    journal_name: Optional[str]
    title: str
    text: str
    published_at: Optional[date]
    flag: bool
    index_text: str
    role: str


class ArticleFullOutput(ArticleOutput):
    identifiers: Optional[List[ArticleIdentifierOutput]] = []
    coi_statement: Optional[CoiStatementOutput] = None
    individual_coi_statements: Optional[List[CoiStatementOutput]] = []


# output for ftm mapping
class PublisherFtm(BaseModel):
    journal_name: str
    journal_id: str


class ArticleFtm(BaseModel):
    article_id: str
    article_title: str
    article_published_at: str
    article_abstract: Optional[str] = None
    article_index_text: Optional[str] = None
    article_authors: Optional[str] = None
    journal_name: str


class ArticleIdentifierFtm(BaseModel):  # Note
    articleidentifier_id: str
    article_id: str
    articleidentifier_label: str
    articleidentifier_value: str


class ArticlePublishedFtm(BaseModel):  # Documentation
    journal_id: str
    article_id: str
    article_published_at: str


class AuthorFtm(BaseModel):
    author_id: str
    author_name: str
    author_first_name: str
    author_middle_names: Optional[str] = None
    author_last_name: str
    author_countries: Optional[str] = None


class AuthorshipFtm(BaseModel):
    author_id: str
    article_id: str
    article_published_at: str
    journal_name: str


class OrganizationFtm(BaseModel):
    institution_id: str
    institution_name: str
    institution_country: Optional[str]


class MembershipFtm(BaseModel):
    author_id: str
    institution_id: str
    article_published_at: str
    journal_name: str


class CoiStatementFtm(BaseModel):  # PlainText
    coi_id: str
    coi_title: str
    coi_text: str
    coi_journal_name: str
    coi_article_id: str
    coi_published_at: str
    coi_author_id: Optional[str]
    coi_author_name: Optional[str]
    coi_authors: Optional[str]
    coi_flag: str
    coi_index_text: str
    coi_role: str
