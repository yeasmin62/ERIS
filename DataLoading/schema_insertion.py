

def schemaInsert(conn):
    sqlfunc = ['''drop aggregate if exists keep_any(double precision);
    drop function if exists keep_any(a double precision, b double precision);

    create or replace function keep_any(a double precision, b double precision) returns
    double precision as $$
    begin
      if (a<>0) THEN return a;
      else return b;
      end if;
    end;
  $$ language plpgsql strict;

  CREATE AGGREGATE keep_any (double precision)
  (
    sfunc = keep_any,
    stype = double precision,
    initcond = 0
        );''', '''drop aggregate if exists sum(sparsevec);
      drop aggregate if exists keep_any(sparsevec);
      drop function if exists scalar_product(v sparsevec, s double precision);
      drop function if exists pairwise_sum(a sparsevec, b sparsevec);
      drop function if exists keep_any(a sparsevec, b sparsevec);
      drop type if exists sparsevec;
      drop type if exists term;

      CREATE TYPE term AS (
        coeff double precision,
        varid text
      );

      CREATE TYPE sparsevec AS (
        terms term[],
        constant double precision
      );

      create or replace function scalar_product(v sparsevec, s double precision) returns
      sparsevec as $$
        begin 
          if (s=0) then 
            return ROW(array[]::term[],0)::sparsevec;
          else
            return row(
                    array (
                      select row(s*coeff,varid)::term 
                      from unnest(v.terms)
                      )::term[],
                    s*v.constant
                  )::sparsevec;
          end if;
        end;
      $$ language plpgsql;

      create or replace function pairwise_sum(a sparsevec, b sparsevec) returns
      sparsevec as $$
        begin 
        return row(
          array (
            select row(sum(coeff),varid)::term 
            from ( 
              select * from unnest(a.terms)
              union all
              select * from unnest(b.terms)
              ) _
            group by varid
            having sum(coeff)<>0
            )::term[],
          a.constant+b.constant
          )::sparsevec;
        end;
      $$ language plpgsql strict;
      CREATE AGGREGATE sum (sparsevec)
      (
        sfunc = pairwise_sum,
        stype = sparsevec,
        initcond = '({},0)'
      );

      create or replace function keep_any(a sparsevec, b sparsevec) returns
      sparsevec as $$
        begin
          if (a<>ROW(array[]::term[],0)::sparsevec) THEN return a;
          else return b;
          end if;
        end;
      $$ language plpgsql strict;

      CREATE AGGREGATE keep_any (sparsevec)
      (
        sfunc = keep_any,
        stype = sparsevec,
        initcond = '({},0)'
      );''']
    
    for query in sqlfunc:
        conn.execute(query)


    factschemaInsertion = ['''insert into schema (tablename, fieldname, key, varfree)
    values ('Copernicus_Temperature', 'date', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Copernicus_Temperature', 'latitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Copernicus_Temperature', 'longitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Copernicus_Temperature', 'Sea_surface_temperature', false, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Climate_Temperature', 'date', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Climate_Temperature', 'latitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Climate_Temperature', 'longitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Climate_Temperature', 'Sea_surface_temperature', false, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Modisaqua_Temperature', 'date', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Modisaqua_Temperature', 'latitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Modisaqua_Temperature', 'longitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Modisaqua_Temperature', 'Sea_surface_temperature', false, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Pathfinder_Temperature', 'date', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Pathfinder_Temperature', 'latitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Pathfinder_Temperature', 'longitude', true, false);''',
    '''insert into schema (tablename, fieldname, key, varfree)
    values ('Pathfinder_Temperature', 'Sea_surface_temperature', false, false);''']

    for query in factschemaInsertion:
        conn.execute(query)

    dimschemaInsertion = ['''insert into schema (tablename, fieldname, key, varfree)
  values ('Dates', 'id', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Dates', 'week', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Dates', 'month', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Dates', 'monthofyear', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Dates', 'year', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Semiday', 'id', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Semiday', 'dateofsemiday', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Semiday', 'time', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution005', 'Degree005', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution005', 'Degree020', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution004', 'Degree004', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution004', 'Degree020', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution020', 'Degree020', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution001', 'Degree001', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution001', 'Degree004', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution001', 'Degree005', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Resolution001', 'Degree020', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Months', 'id', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Months', 'monthofyear', true, false);''',
  '''insert into schema (tablename, fieldname, key, varfree)
  values ('Months', 'year', true, false);''']

    for query in dimschemaInsertion:
      conn.execute(query)



    