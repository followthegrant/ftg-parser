begin;

create table if not exists author_triples (
  fingerprint char(40) not null,
  author_id char(40) not null,
  value_id char(40) not null,
  dataset varchar(20),
  unique (fingerprint, author_id, value_id, dataset)
);

create index on author_triples (fingerprint);
create index on author_triples (author_id);
create index on author_triples (dataset);

create table if not exists author_aggregation (
  agg_id char(40) not null,
  author_id char(40) not null,
  dataset varchar(20),
  unique (agg_id, author_id, dataset)
);

create index on author_aggregation (agg_id);
create index on author_aggregation (author_id);
create index on author_aggregation (dataset);

commit;
