-- journals
create unique index on @dataset_journals (id);

-- articles
create unique index on @dataset_articles (id);

-- article ids for crossmatch
create index on @dataset_article_identifiers (article_id);
create index on @dataset_article_identifiers (key);
create index on @dataset_article_identifiers (id);

-- authors
create unique index on @dataset_authors (id);
create index on @dataset_authors (fingerprint);
create index on @dataset_authors (country);

-- institutions
create unique index on @dataset_institutions (id);
create index on @dataset_institutions (country);

-- author affiliations
create index on @dataset_affiliations (author_id);
create index on @dataset_affiliations (institution_id);

-- authorship
create index on @dataset_authorship (author_id);
create index on @dataset_authorship (article_id);

-- coi statements
create index on @dataset_cois (article_id);
create index on @dataset_cois (author_id);
create index on @dataset_cois (coi_id);
create index on @dataset_cois (type);

-- acknowledgement statements
create index on @dataset_acks (article_id);
create index on @dataset_acks (author_id);
create index on @dataset_acks (ack_id);
create index on @dataset_acks (type);

-- mentions
create index on @dataset_mentions (document_id);
create index on @dataset_mentions (mention);
