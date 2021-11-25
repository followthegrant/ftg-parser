begin;

drop table if exists @dataset_journals;
drop table if exists @dataset_articles;
drop table if exists @dataset_article_identifiers;
drop table if exists @dataset_authors;
drop table if exists @dataset_institutions;
drop table if exists @dataset_affiliations;
drop table if exists @dataset_documentation;
drop table if exists @dataset_authorship;
drop table if exists @dataset_cois;

-- intermediary table
create table @dataset_documentation as (
  select distinct
    entity -> 'properties' -> 'entity' ->> 0 as entity,
    entity -> 'properties' -> 'document' ->> 0 as document,
    entity -> 'properties' -> 'role' ->> 0 as role,
    entity -> 'properties' -> 'date' ->> 0 as date
  from @collection
  where
    origin = 'aggregated'
    and entity > '{"schema": "Documentation"}'
);
create index on @dataset_documentation (entity);
create index on @dataset_documentation (document);
create index on @dataset_documentation (role);

-- journals
create table @dataset_journals as (
  select distinct
    split_part(id, '.', 1) as id,
    entity -> 'properties' -> 'name' ->> 0 as name,
    entity -> 'properties' -> 'name' as names
  from @collection
  where
    origin = 'aggregated'
    and entity @> '{"schema": "LegalEntity"}'
);
create unique index on @dataset_journals (id);

-- articles
create table @dataset_articles as (
  select distinct
    split_part(a.id, '.', 1) as id,
    a.entity -> 'properties' -> 'publishedAt' ->> 0 as published_at,
    a.entity -> 'properties' -> 'title' ->> 0 as title,
    a.entity -> 'properties' -> 'publisher' ->> 0 as publisher,
    (c.id is not null)::int as has_coi,
    (c.entity -> 'properties' -> 'notes' ->> 0)::int as has_conflict
  from @collection a
  left join @dataset_documentation b
    on a.id = b.document
    and b.role = 'conflict of interest statement (article)'
  left join @collection c
    on b.entity = c.id
    and c.entity @> '{"schema": "PlainText"}'
  where
    a.origin = 'aggregated'
    and a.entity @> '{"schema": "Article"}'
);
create unique index on @dataset_articles (id);

-- article ids for crossmatch
create table @dataset_article_identifiers as (
  select distinct
    split_part(entity -> 'properties' -> 'entity' ->> 0, '.', 1) as article_id,
    entity -> 'properties' -> 'description' ->> 0 as id,
    entity -> 'properties' -> 'summary' ->> 0 as key
  from @collection
  where
    origin = 'aggregated'
    and entity @> '{"schema": "Note"}'
);
create index on @dataset_article_identifiers (article_id);
create index on @dataset_article_identifiers (key);
create index on @dataset_article_identifiers (id);

-- authors
create table @dataset_authors as (
  select distinct
    split_part(id, '.', 1) as id,
    entity -> 'properties' -> 'name' ->> 0 as name,
    entity -> 'properties' -> 'name' as names,
    entity -> 'properties' -> 'country' ->> 0 as country,
    entity -> 'properties' -> 'country' as countries
  from @collection
  where
    origin = 'aggregated'
    and entity @> '{"schema": "Person"}'
);
create unique index on @dataset_authors (id);
create index on @dataset_authors (country, countries);

-- institutions
create table @dataset_institutions as (
  select distinct
    split_part(id, '.', 1) as id,
    entity -> 'properties' -> 'name' ->> 0 as name,
    entity -> 'properties' -> 'name' as names,
    entity -> 'properties' -> 'country' ->> 0 as country,
    entity -> 'properties' -> 'country' as countries
  from @collection
  where
    origin = 'aggregated'
    and entity @> '{"schema": "Organization"}'
);
create unique index on @dataset_institutions (id);
create index on @dataset_institutions (country, countries);

-- author affiliations
create table @dataset_affiliations as (
  select distinct
    split_part(entity -> 'properties' -> 'member' ->> 0, '.', 1) as author_id,
    split_part(entity -> 'properties' -> 'organization' ->> 0, '.', 1) as institution_id,
    entity -> 'properties' -> 'date' ->> 0 as date,
    entity -> 'properties' -> 'date' as dates
  from @collection
  where
    origin = 'aggregated'
    and entity @> '{"schema": "Membership"}'
);

-- authorship
create table @dataset_authorship as (
  select distinct
    split_part(entity, '.', 1) as author_id,
    split_part(document, '.', 1) as article_id,
    date
  from @dataset_documentation
  where
    role = 'author'
);
create index on @dataset_authorship (author_id);
create index on @dataset_authorship (article_id);

-- coi statements
create table @dataset_cois as (
  select distinct
    split_part(a.document, '.', 1) as article_id,
    split_part(a.entity, '.', 1) as coi_id,
    split_part(b.entity, '.', 1) as author_id,
    a.role as type,
    a.date,
    c.entity -> 'properties' -> 'bodyText' ->> 0 as statement,
    (c.entity -> 'properties' -> 'notes' ->> 0)::int as flag
  from @dataset_documentation a
  join @dataset_documentation b
    on a.entity = b.document
  left join @collection c
    on c.id = a.entity
    and c.origin = 'aggregated'
);
create index on @dataset_cois (article_id);
create index on @dataset_cois (author_id);
create index on @dataset_cois (coi_id);
create index on @dataset_cois (type);

commit;
