begin;

create table if not exists author_triples (
  fingerprint char(40) not null,
  author_id char(40) not null,
  value_id char(40) not null,
  source varchar(20) not null,
  unique (fingerprint, author_id, value_id, source)
);

create index on author_triples (fingerprint);
create index on author_triples (author_id);
create index on author_triples (value_id);
create index on author_triples (source);

create table if not exists author_aggregation (
  agg_id char(40) not null,
  author_id char(40) not null,
  unique (agg_id, author_id)
);

create index on author_aggregation (agg_id);
create index on author_aggregation (author_id);

commit;
