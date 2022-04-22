begin;

create table if not exists statements (
  id char(40) not null,
  entity_id char(40) not null,
  canonical_id char(40) not null,
  prop varchar(40) not null,
  prop_type varchar(20) not null,
  schema varchar(20) not null,
  value text not null,
  dataset varchar(20) not null,
  first_seen timestamp not null,
  last_seen timestamp not null
);

commit;

begin;

create unique index on statements (id);
create index on statements (entity_id);
create index on statements (prop);
create index on statements (schema);
/* create index on statements (value); */
create index on statements (dataset);

commit;
