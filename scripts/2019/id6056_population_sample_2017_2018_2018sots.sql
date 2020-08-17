select
  'population' as cut,
  count(d.district_id) as num_districts,
  sum(d.num_schools) as num_schools,
  sum(d.num_campuses) as num_campuses,
  sum(d.num_students) as num_students,
  count(case
          when dfs.district_type = 'Traditional'
            then d.district_id
        end) as num_districts_overlap,
  sum(case
        when dfs.district_type = 'Traditional'
          then d.num_schools
      end) as num_schools_overlap,
  sum(case
        when dfs.district_type = 'Traditional'
          then d.num_campuses
      end) as num_campuses_overlap,
  sum(case
        when dfs.district_type = 'Traditional'
          then d.num_students
      end) as num_students_overlap
from ps.districts d 
left join ps.districts_frozen_sots dfs
on d.district_id = dfs.district_id
and d.funding_year = dfs.funding_year + 1
where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2019

UNION

select
  'sample' as cut,
  count(d.district_id) as num_districts,
  sum(d.num_schools) as num_schools,
  sum(d.num_campuses) as num_campuses,
  sum(d.num_students) as num_students,
  count(case
          when dfs.district_type = 'Traditional'
          and dffafs.fit_for_ia = true
            then d.district_id
        end) as num_districts_overlap,
  sum(case
        when dfs.district_type = 'Traditional'
        and dffafs.fit_for_ia = true
          then d.num_schools
      end) as num_schools_overlap,
  sum(case
        when dfs.district_type = 'Traditional'
        and dffafs.fit_for_ia = true
          then d.num_campuses
      end) as num_campuses_overlap,
  sum(case
        when dfs.district_type = 'Traditional'
        and dffafs.fit_for_ia = true
          then d.num_students
      end) as num_students_overlap
from ps.districts d 
join ps.districts_fit_for_analysis dffa
on d.district_id = dffa.district_id
and d.funding_year = dffa.funding_year
left join ps.districts_frozen_sots dfs
on d.district_id = dfs.district_id
and d.funding_year = dfs.funding_year + 1
left join ps.districts_fit_for_analysis_frozen_sots dffafs
on dfs.district_id = dffafs.district_id
and dfs.funding_year = dffafs.funding_year
where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2019
and dffa.fit_for_ia = true