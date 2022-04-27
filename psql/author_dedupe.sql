begin;

drop table if exists author_triples;
drop table if exists author_aggregation;

create table author_triples (
  fingerprint char(40) not null,
  author_id char(40) not null,
  value_id char(40) not null,
  dataset varchar(20),
  unique (fingerprint, author_id, value_id, dataset)
);

create index author_triples_fp_x on author_triples (fingerprint, dataset);
create index author_triples_id_x on author_triples (author_id, dataset);
create index author_triples_ds_x on author_triples (dataset);

create table author_aggregation (
  agg_id char(40) not null,
  author_id char(40) not null,
  dataset varchar(20),
  unique (agg_id, author_id, dataset)
);

create index author_aggregation_agg_x on author_aggregation (agg_id, dataset);
create index author_aggregatiio_id_x on author_aggregation (author_id, dataset);
create index author_aggregation_ds_x on author_aggregation (dataset);

commit;
