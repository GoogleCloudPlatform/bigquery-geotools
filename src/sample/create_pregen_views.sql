create materialized view if not exists `PROJECT.DATASET.BASE_TABLE_pregen_1m`
cluster by geom
as (
  select
    * except(geom),
    st_simplify(geom, 1) as geom,
    st_asgeojson(st_simplify(geom, 1)) as geom_geojson
  from `PROJECT.DATASET.BASE_TABLE`
)

create materialized view if not exists `PROJECT.DATASET.BASE_TABLE_pregen_10m`
cluster by geom
as (
  select
    * except(geom),
    st_simplify(geom, 10) as geom,
    st_asgeojson(st_simplify(geom, 10)) as geom_geojson
  from `PROJECT.DATASET.BASE_TABLE`
)

create materialized view if not exists `PROJECT.DATASET.BASE_TABLE_pregen_100m`
cluster by geom
as (
  select
    * except(geom),
    st_simplify(geom, 100) as geom,
    st_asgeojson(st_simplify(geom, 100)) as geom_geojson
  from `PROJECT.DATASET.BASE_TABLE`
)
