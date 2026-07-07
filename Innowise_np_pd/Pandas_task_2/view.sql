-- ============================================================================
-- BoM explosion view  (PostgreSQL dialect, uses a recursive CTE)
--
-- One output row = one (material -> component) edge inside a FIN's production
-- tree, per plant and year, with quantities summed over the year. Shared
-- sub-assemblies appear once under every FIN that uses them (top-down walk),
-- and the FIN itself never appears as a produced material (the walk starts
-- below it, at its PROD sub).
-- ============================================================================

CREATE VIEW bom_explosion AS
WITH RECURSIVE

produced AS (
    SELECT DISTINCT produced_material AS material_id
    FROM bom_data
),

fin_tree AS (
    -- Anchor: start below each FIN, at its PROD sub (the FIN's own component),
    -- but only if that component is itself a produced material.
    SELECT DISTINCT
        b.plant_id,
        b.produced_material  AS fin_id,
        b.component_material AS material_id,
        1                    AS depth
    FROM bom_data b
    JOIN produced p ON p.material_id = b.component_material
    WHERE b.produced_material_release_type = 'FIN'
    UNION ALL
    -- Descend: from a material already in the tree to each of its components
    -- that is itself produced. `depth` guards against runaway recursion.
    SELECT
        ft.plant_id,
        ft.fin_id,
        b.component_material AS material_id,
        ft.depth + 1
    FROM fin_tree ft
    JOIN bom_data b
      ON b.produced_material = ft.material_id
     AND b.plant_id          = ft.plant_id
    JOIN produced p ON p.material_id = b.component_material
    WHERE ft.depth < 20
),
-- Collapse the walk to a unique (plant, fin, material) set so a material reached
-- by more than one path under the same FIN is not counted twice.
fin_material AS (
    SELECT DISTINCT plant_id, fin_id, material_id
    FROM fin_tree
),
-- FIN production quantity, summed per plant/year.
fin_quantity AS (
    SELECT plant_id, fin_id, year, SUM(q) AS fin_production_quantity
    FROM (
        SELECT DISTINCT
            plant_id,
            produced_material AS fin_id,
            year,
            month,
            produced_material_quantity AS q
        FROM bom_data
        WHERE produced_material_release_type = 'FIN'
    ) monthly
    GROUP BY plant_id, fin_id, year
),
-- FIN descriptive attributes (release + production type).
fin_attr AS (
    SELECT DISTINCT
        plant_id,
        produced_material                 AS fin_id,
        produced_material_release_type    AS fin_material_release_type,
        produced_material_production_type AS fin_material_production_type
    FROM bom_data
    WHERE produced_material_release_type = 'FIN'
)
-- Attach each mapped material's own component edges, tag with the FIN, and sum
-- the produced/consumption quantities over the year
SELECT
    fm.plant_id                           AS plant,
    fm.fin_id                             AS fin_material_id,
    fa.fin_material_release_type,
    fa.fin_material_production_type,
    fq.fin_production_quantity,
    b.produced_material                   AS prod_material_id,
    b.produced_material_release_type      AS prod_material_release_type,
    b.produced_material_production_type   AS prod_material_production_type,
    SUM(b.produced_material_quantity)     AS prod_material_production_quantity,
    b.component_material                  AS component_id,
    b.component_material_release_type,
    b.component_material_production_type,
    SUM(b.component_material_quantity)    AS component_consumption_quantity,
    b.year
FROM fin_material fm
JOIN bom_data b
ON b.produced_material = fm.material_id AND b.plant_id = fm.plant_id
JOIN fin_attr fa 
ON fa.plant_id = fm.plant_id AND fa.fin_id = fm.fin_id
JOIN fin_quantity fq 
ON fq.plant_id = fm.plant_id AND fq.fin_id = fm.fin_id AND fq.year = b.year
GROUP BY
    fm.plant_id,
    fm.fin_id,
    fa.fin_material_release_type,
    fa.fin_material_production_type,
    fq.fin_production_quantity,
    b.produced_material,
    b.produced_material_release_type,
    b.produced_material_production_type,
    b.component_material,
    b.component_material_release_type,
    b.component_material_production_type,
    b.year
ORDER BY
    fm.plant_id, fm.fin_id, b.produced_material, b.component_material, b.year;



-- SELECT for checking the view
SELECT * FROM bom_explosion LIMIT 500;
